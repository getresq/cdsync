use anyhow::{Context, Result};
use chrono::{DateTime, Local, Utc};
use clap::Args;
use crossterm::cursor::{Hide, Show};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};
use ratatui::{Frame, Terminal};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::io::{self, Stdout};
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_REFRESH_SECONDS: u64 = 2;
const MONITOR_URL_ENV: &str = "CDSYNC_MONITOR_URL";
const MONITOR_PRIVATE_KEY_PATH_ENV: &str = "CDSYNC_MONITOR_PRIVATE_KEY_PATH";
const MONITOR_ISSUER_ENV: &str = "CDSYNC_MONITOR_ISSUER";
const MONITOR_AUDIENCE_ENV: &str = "CDSYNC_MONITOR_AUDIENCE";
const MONITOR_SUBJECT_ENV: &str = "CDSYNC_MONITOR_SUBJECT";
const MONITOR_SCOPE_ENV: &str = "CDSYNC_MONITOR_SCOPE";
const MONITOR_KID_ENV: &str = "CDSYNC_MONITOR_KID";
const MONITOR_EXPIRES_IN_SECONDS_ENV: &str = "CDSYNC_MONITOR_EXPIRES_IN_SECONDS";
const DEFAULT_MONITOR_ISSUER: &str = "ops-cli";
const DEFAULT_MONITOR_AUDIENCE: &str = "cdsync";
const DEFAULT_MONITOR_KID_SUFFIX: &str = "20260401";

#[derive(Args, Debug, Clone)]
struct MonitorAuthArgs {
    #[arg(long)]
    private_key_path: Option<String>,
    #[arg(long)]
    issuer: Option<String>,
    #[arg(long)]
    audience: Option<String>,
    #[arg(long)]
    subject: Option<String>,
    #[arg(long, default_value = "cdsync:admin")]
    scope: String,
    #[arg(long)]
    kid: Option<String>,
    #[arg(long, default_value_t = 300)]
    expires_in_seconds: u64,
}

#[derive(Args, Debug, Clone)]
pub struct MonitorArgs {
    #[arg(long)]
    pub url: Option<String>,
    #[arg(long)]
    pub connection: Option<String>,
    #[arg(long, default_value_t = DEFAULT_REFRESH_SECONDS)]
    pub refresh_seconds: u64,
    #[arg(long)]
    pub plain: bool,
    #[command(flatten)]
    auth: MonitorAuthArgs,
}

#[derive(Clone)]
struct MonitorApiClient {
    client: reqwest::Client,
    base_url: String,
    auth: MonitorAuthConfig,
}

#[derive(Clone)]
struct MonitorAuthConfig {
    encoding_key: EncodingKey,
    issuer: String,
    audience: String,
    subject: String,
    scope: String,
    kid: String,
    expires_in_seconds: u64,
}

#[derive(Debug, Clone)]
struct MonitorSnapshot {
    status: StatusResponse,
    connections: Vec<ConnectionSummary>,
    selected_connection_id: Option<String>,
    progress: Option<ProgressResponse>,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct MonitorViewState {
    selected_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct StatusResponse {
    service: String,
    version: String,
    started_at: DateTime<Utc>,
    mode: String,
    connection_id: String,
    connection_count: usize,
    config_hash: String,
    deploy_revision: Option<String>,
    last_restart_reason: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ConnectionSummary {
    id: String,
    enabled: bool,
    source_kind: String,
    destination_kind: String,
    last_sync_started_at: Option<DateTime<Utc>>,
    last_sync_finished_at: Option<DateTime<Utc>>,
    last_sync_status: Option<String>,
    last_error: Option<String>,
    phase: String,
    reason_code: String,
    max_checkpoint_age_seconds: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProgressResponse {
    connection_id: String,
    runtime: ConnectionRuntime,
    current_run: Option<RunSummary>,
    tables: Vec<TableProgress>,
}

#[derive(Debug, Clone, Deserialize)]
struct ConnectionRuntime {
    connection_id: String,
    phase: String,
    reason_code: String,
    last_sync_started_at: Option<DateTime<Utc>>,
    last_sync_finished_at: Option<DateTime<Utc>>,
    last_sync_status: Option<String>,
    last_error: Option<String>,
    max_checkpoint_age_seconds: Option<i64>,
    config_hash: String,
    deploy_revision: Option<String>,
    last_restart_reason: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RunSummary {
    run_id: String,
    connection_id: String,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    status: Option<String>,
    error: Option<String>,
    rows_read: i64,
    rows_written: i64,
    rows_deleted: i64,
    rows_upserted: i64,
    extract_ms: i64,
    load_ms: i64,
    api_calls: i64,
    rate_limit_hits: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct TableProgress {
    table_name: String,
    phase: String,
    reason_code: String,
    checkpoint_age_seconds: Option<i64>,
    lag_seconds: Option<i64>,
    snapshot_chunks_total: usize,
    snapshot_chunks_complete: usize,
    checkpoint: Option<TableCheckpointSummary>,
    stats: Option<TableStatsSummary>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableCheckpointSummary {
    last_primary_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableStatsSummary {
    rows_read: i64,
    rows_written: i64,
    rows_deleted: i64,
    rows_upserted: i64,
}

#[derive(Serialize, Deserialize)]
struct MonitorServiceJwtClaims {
    exp: usize,
    iat: usize,
    iss: String,
    aud: String,
    sub: String,
    jti: String,
    scope: String,
}

pub async fn cmd_monitor(args: MonitorArgs) -> Result<()> {
    let client = MonitorApiClient::from_args(&args)?;
    if args.plain {
        let snapshot = client.fetch_snapshot(args.connection.as_deref()).await?;
        print!("{}", render_plain(&snapshot));
        return Ok(());
    }

    run_tui(
        client,
        args.connection,
        Duration::from_secs(args.refresh_seconds.max(1)),
    )
    .await
}

impl MonitorApiClient {
    fn from_args(args: &MonitorArgs) -> Result<Self> {
        let base_url = args
            .url
            .clone()
            .or_else(|| std::env::var(MONITOR_URL_ENV).ok())
            .context(format!(
                "--url or {} is required for cdsync monitor",
                MONITOR_URL_ENV
            ))?;
        let base_url = base_url.trim_end_matches('/').to_string();
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(format!("cdsync-monitor/{}", env!("CARGO_PKG_VERSION")))
                .build()
                .context("building monitor HTTP client")?,
            base_url,
            auth: MonitorAuthConfig::from_args(&args.auth)?,
        })
    }

    async fn fetch_snapshot(
        &self,
        selected_connection_id: Option<&str>,
    ) -> Result<MonitorSnapshot> {
        let status: StatusResponse = self.get_json("/v1/status").await?;
        let connections: Vec<ConnectionSummary> = self.get_json("/v1/connections").await?;
        let selected_connection_id =
            resolve_selected_connection_id(&connections, selected_connection_id)?;
        let progress = if let Some(connection_id) = &selected_connection_id {
            Some(
                self.get_json::<ProgressResponse>(&format!(
                    "/v1/connections/{connection_id}/progress"
                ))
                .await?,
            )
        } else {
            None
        };
        Ok(MonitorSnapshot {
            status,
            connections,
            selected_connection_id,
            progress,
            fetched_at: Utc::now(),
        })
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let token = self.auth.mint_token()?;
        let response = self
            .client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("requesting {}", path))?;

        if response.status() == StatusCode::UNAUTHORIZED {
            anyhow::bail!("admin API returned 401 for {}; check bearer token", path);
        }
        if response.status() == StatusCode::FORBIDDEN {
            anyhow::bail!(
                "admin API returned 403 for {}; token lacks required scope",
                path
            );
        }

        let response = response
            .error_for_status()
            .with_context(|| format!("requesting {}", path))?;
        response
            .json::<T>()
            .await
            .with_context(|| format!("decoding {}", path))
    }
}

impl MonitorAuthConfig {
    fn from_args(args: &MonitorAuthArgs) -> Result<Self> {
        let private_key_path = args
            .private_key_path
            .clone()
            .or_else(|| std::env::var(MONITOR_PRIVATE_KEY_PATH_ENV).ok())
            .context(format!(
                "--private-key-path or {} is required for cdsync monitor",
                MONITOR_PRIVATE_KEY_PATH_ENV
            ))?;
        let issuer = args
            .issuer
            .clone()
            .or_else(|| std::env::var(MONITOR_ISSUER_ENV).ok())
            .unwrap_or_else(|| DEFAULT_MONITOR_ISSUER.to_string());
        let audience = args
            .audience
            .clone()
            .or_else(|| std::env::var(MONITOR_AUDIENCE_ENV).ok())
            .unwrap_or_else(|| DEFAULT_MONITOR_AUDIENCE.to_string());
        let kid = args
            .kid
            .clone()
            .or_else(|| std::env::var(MONITOR_KID_ENV).ok())
            .unwrap_or_else(|| format!("{issuer}-{DEFAULT_MONITOR_KID_SUFFIX}"));
        if !kid.starts_with(&format!("{issuer}-")) {
            anyhow::bail!("monitor JWT kid must start with `{issuer}-`");
        }
        let subject = args
            .subject
            .clone()
            .or_else(|| std::env::var(MONITOR_SUBJECT_ENV).ok())
            .unwrap_or_else(|| format!("{issuer}:monitor"));
        let scope = if args.scope == "cdsync:admin" {
            std::env::var(MONITOR_SCOPE_ENV).unwrap_or_else(|_| args.scope.clone())
        } else {
            args.scope.clone()
        };
        let expires_in_seconds = if args.expires_in_seconds == 300 {
            std::env::var(MONITOR_EXPIRES_IN_SECONDS_ENV)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(args.expires_in_seconds)
        } else {
            args.expires_in_seconds
        };
        let private_key_bytes = std::fs::read(&private_key_path)
            .with_context(|| format!("reading monitor private key {}", private_key_path))?;
        let encoding_key = EncodingKey::from_rsa_pem(&private_key_bytes)
            .context("parsing monitor RSA private key")?;
        Ok(Self {
            encoding_key,
            issuer,
            audience,
            subject,
            scope,
            kid,
            expires_in_seconds: expires_in_seconds.max(30),
        })
    }

    fn mint_token(&self) -> Result<String> {
        let now = Utc::now().timestamp() as usize;
        let claims = MonitorServiceJwtClaims {
            exp: now + self.expires_in_seconds as usize,
            iat: now,
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            sub: self.subject.clone(),
            jti: Uuid::new_v4().to_string(),
            scope: self.scope.clone(),
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        encode(&header, &claims, &self.encoding_key).context("encoding monitor JWT")
    }
}

async fn run_tui(
    client: MonitorApiClient,
    requested_connection_id: Option<String>,
    refresh_interval: Duration,
) -> Result<()> {
    let mut terminal = TerminalSession::new()?;
    let mut view_state = MonitorViewState { selected_index: 0 };
    let mut snapshot = client
        .fetch_snapshot(requested_connection_id.as_deref())
        .await?;
    reconcile_selection(
        &mut view_state,
        &snapshot,
        requested_connection_id.as_deref(),
    );
    let mut last_error: Option<String> = None;
    let mut next_refresh = tokio::time::Instant::now() + refresh_interval;

    loop {
        terminal.draw(|frame| {
            render(
                frame,
                &snapshot,
                &view_state,
                client.base_url.as_str(),
                refresh_interval,
                last_error.as_deref(),
            )
        })?;

        let timeout = next_refresh
            .saturating_duration_since(tokio::time::Instant::now())
            .min(Duration::from_millis(250));
        let event = tokio::task::spawn_blocking(move || -> Result<Option<Event>> {
            if event::poll(timeout)? {
                Ok(Some(event::read()?))
            } else {
                Ok(None)
            }
        })
        .await
        .context("joining monitor event task")??;

        if let Some(Event::Key(key)) = event
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                KeyCode::Down | KeyCode::Char('j') => {
                    move_selection(&mut view_state, &snapshot, 1);
                    next_refresh = tokio::time::Instant::now();
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    move_selection(&mut view_state, &snapshot, -1);
                    next_refresh = tokio::time::Instant::now();
                }
                KeyCode::Home | KeyCode::Char('g') => {
                    view_state.selected_index = 0;
                    next_refresh = tokio::time::Instant::now();
                }
                KeyCode::End | KeyCode::Char('G') => {
                    if !snapshot.connections.is_empty() {
                        view_state.selected_index = snapshot.connections.len() - 1;
                    }
                    next_refresh = tokio::time::Instant::now();
                }
                KeyCode::Char('r') => next_refresh = tokio::time::Instant::now(),
                _ => {}
            }
        }

        if tokio::time::Instant::now() >= next_refresh {
            match client
                .fetch_snapshot(current_selected_connection_id(&snapshot, &view_state))
                .await
            {
                Ok(new_snapshot) => {
                    snapshot = new_snapshot;
                    reconcile_selection(&mut view_state, &snapshot, None);
                    last_error = None;
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                }
            }
            next_refresh = tokio::time::Instant::now() + refresh_interval;
        }
    }

    Ok(())
}

fn move_selection(view_state: &mut MonitorViewState, snapshot: &MonitorSnapshot, delta: isize) {
    if snapshot.connections.is_empty() {
        view_state.selected_index = 0;
        return;
    }
    let len = snapshot.connections.len() as isize;
    let next = (view_state.selected_index as isize + delta).clamp(0, len - 1);
    view_state.selected_index = next as usize;
}

fn reconcile_selection(
    view_state: &mut MonitorViewState,
    snapshot: &MonitorSnapshot,
    requested_connection_id: Option<&str>,
) {
    if snapshot.connections.is_empty() {
        view_state.selected_index = 0;
        return;
    }
    if let Some(requested) = requested_connection_id
        && let Some(index) = snapshot
            .connections
            .iter()
            .position(|connection| connection.id == requested)
    {
        view_state.selected_index = index;
        return;
    }
    if let Some(selected_connection_id) = &snapshot.selected_connection_id
        && let Some(index) = snapshot
            .connections
            .iter()
            .position(|connection| connection.id == *selected_connection_id)
    {
        view_state.selected_index = index;
        return;
    }
    view_state.selected_index = view_state
        .selected_index
        .min(snapshot.connections.len().saturating_sub(1));
}

fn resolve_selected_connection_id(
    connections: &[ConnectionSummary],
    requested_connection_id: Option<&str>,
) -> Result<Option<String>> {
    if let Some(requested) = requested_connection_id {
        if connections
            .iter()
            .any(|connection| connection.id == requested)
        {
            return Ok(Some(requested.to_string()));
        }
        anyhow::bail!("connection not found: {}", requested);
    }
    Ok(connections.first().map(|connection| connection.id.clone()))
}

fn current_selected_connection_id<'a>(
    snapshot: &'a MonitorSnapshot,
    view_state: &MonitorViewState,
) -> Option<&'a str> {
    snapshot
        .connections
        .get(view_state.selected_index)
        .map(|connection| connection.id.as_str())
        .or(snapshot.selected_connection_id.as_deref())
}

fn render(
    frame: &mut Frame<'_>,
    snapshot: &MonitorSnapshot,
    view_state: &MonitorViewState,
    base_url: &str,
    refresh_interval: Duration,
    last_error: Option<&str>,
) {
    let area = frame.area();
    let layout = main_layout(area);

    render_header(frame, layout[0], snapshot, base_url, refresh_interval);
    render_body(frame, layout[1], snapshot, view_state);
    render_footer(frame, layout[2], last_error);
}

fn main_layout(area: Rect) -> std::rc::Rc<[Rect]> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(12),
            Constraint::Length(3),
        ])
        .split(area)
}

fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &MonitorSnapshot,
    base_url: &str,
    refresh_interval: Duration,
) {
    let header = vec![
        Line::from(vec![
            Span::styled(
                " CDSync Monitor ",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(
                " url: {}  refresh: {}s",
                base_url,
                refresh_interval.as_secs()
            )),
        ]),
        Line::from(format!(
            " service: {} {}   mode: {}   managed: {}   started: {}",
            snapshot.status.service,
            snapshot.status.version,
            snapshot.status.mode,
            snapshot.status.connection_count,
            format_timestamp(snapshot.status.started_at)
        )),
        Line::from(format!(
            " config: {}   deploy: {}   restart: {}   default: {}   fetched: {}",
            snapshot.status.config_hash,
            snapshot.status.deploy_revision.as_deref().unwrap_or("-"),
            snapshot.status.last_restart_reason,
            snapshot.status.connection_id,
            format_timestamp(snapshot.fetched_at)
        )),
    ];

    let paragraph = Paragraph::new(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(border::ROUNDED)
                .title(" Overview "),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_body(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &MonitorSnapshot,
    view_state: &MonitorViewState,
) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(area);

    render_connections(frame, columns[0], snapshot, view_state);
    render_details(frame, columns[1], snapshot);
}

fn render_connections(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &MonitorSnapshot,
    view_state: &MonitorViewState,
) {
    let rows: Vec<Row> = snapshot
        .connections
        .iter()
        .enumerate()
        .map(|(index, connection)| {
            let style = if index == view_state.selected_index {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(connection.id.clone()),
                Cell::from(connection.phase.clone()),
                Cell::from(format_age(connection.max_checkpoint_age_seconds)),
                Cell::from(
                    connection
                        .last_sync_status
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(38),
            Constraint::Percentage(30),
            Constraint::Length(9),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec!["Connection", "Phase", "Age", "Status"]).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_set(border::ROUNDED)
            .title(" Connections "),
    );
    frame.render_widget(table, area);
}

fn render_details(frame: &mut Frame<'_>, area: Rect, snapshot: &MonitorSnapshot) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Min(8),
        ])
        .split(area);

    render_runtime(frame, sections[0], snapshot);
    render_run(frame, sections[1], snapshot);
    render_tables(frame, sections[2], snapshot);
}

fn render_runtime(frame: &mut Frame<'_>, area: Rect, snapshot: &MonitorSnapshot) {
    let lines = if let Some(progress) = &snapshot.progress {
        vec![
            Line::from(format!(" connection: {}", progress.connection_id)),
            Line::from(format!(
                " phase: {}   reason: {}   age: {}",
                progress.runtime.phase,
                progress.runtime.reason_code,
                format_age(progress.runtime.max_checkpoint_age_seconds),
            )),
            Line::from(format!(
                " last status: {}   started: {}   finished: {}",
                progress.runtime.last_sync_status.as_deref().unwrap_or("-"),
                progress
                    .runtime
                    .last_sync_started_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "-".to_string()),
                progress
                    .runtime
                    .last_sync_finished_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "-".to_string()),
            )),
            Line::from(format!(
                " runtime connection: {}   config: {}   deploy: {}",
                progress.runtime.connection_id,
                progress.runtime.config_hash,
                progress.runtime.deploy_revision.as_deref().unwrap_or("-"),
            )),
            Line::from(format!(
                " restart reason: {}   last error: {}",
                progress.runtime.last_restart_reason,
                progress.runtime.last_error.as_deref().unwrap_or("-"),
            )),
        ]
    } else {
        vec![Line::from(" no connection selected ")]
    };

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(border::ROUNDED)
                .title(" Runtime "),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_run(frame: &mut Frame<'_>, area: Rect, snapshot: &MonitorSnapshot) {
    let lines = if let Some(run) = snapshot
        .progress
        .as_ref()
        .and_then(|progress| progress.current_run.as_ref())
    {
        vec![
            Line::from(format!(" run: {}", run.run_id)),
            Line::from(format!(
                " status: {}   started: {}   finished: {}",
                run.status.as_deref().unwrap_or("-"),
                format_timestamp(run.started_at),
                run.finished_at
                    .map(format_timestamp)
                    .unwrap_or_else(|| "-".to_string()),
            )),
            Line::from(format!(
                " connection: {}   rows read: {}   written: {}   upserted: {}   deleted: {}",
                run.connection_id,
                run.rows_read,
                run.rows_written,
                run.rows_upserted,
                run.rows_deleted
            )),
            Line::from(format!(
                " extract_ms: {}   load_ms: {}   api_calls: {}   rate_limit_hits: {}",
                run.extract_ms, run.load_ms, run.api_calls, run.rate_limit_hits
            )),
            Line::from(format!(" error: {}", run.error.as_deref().unwrap_or("-"))),
        ]
    } else {
        vec![Line::from(" no active run details available ")]
    };

    let paragraph = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(border::ROUNDED)
                .title(" Current Run "),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_tables(frame: &mut Frame<'_>, area: Rect, snapshot: &MonitorSnapshot) {
    let rows: Vec<Row> = snapshot
        .progress
        .as_ref()
        .map(|progress| {
            progress
                .tables
                .iter()
                .map(|table| {
                    Row::new(vec![
                        Cell::from(table.table_name.clone()),
                        Cell::from(table.phase.clone()),
                        Cell::from(format_age(table.lag_seconds)),
                        Cell::from(if table.snapshot_chunks_total > 0 {
                            format!(
                                "{}/{}",
                                table.snapshot_chunks_complete, table.snapshot_chunks_total
                            )
                        } else {
                            "-".to_string()
                        }),
                        Cell::from(
                            table
                                .checkpoint
                                .as_ref()
                                .and_then(|checkpoint| checkpoint.last_primary_key.clone())
                                .unwrap_or_else(|| "-".to_string()),
                        ),
                    ])
                })
                .collect()
        })
        .unwrap_or_default();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(46),
            Constraint::Percentage(18),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Percentage(19),
        ],
    )
    .header(
        Row::new(vec!["Table", "Phase", "Lag", "Chunks", "Last PK"]).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_set(border::ROUNDED)
            .title(" Tables "),
    );
    frame.render_widget(table, area);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, last_error: Option<&str>) {
    frame.render_widget(Clear, area);
    let message = last_error.unwrap_or("q quit   ↑↓/j/k move   r refresh");
    let style = if last_error.is_some() {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let paragraph = Paragraph::new(message).style(style).block(
        Block::default()
            .borders(Borders::ALL)
            .border_set(border::ROUNDED),
    );
    frame.render_widget(paragraph, area);
}

fn render_plain(snapshot: &MonitorSnapshot) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "CDSync {}  mode={}  connections={}  config={}  deploy={}  started={}  default={}",
        snapshot.status.version,
        snapshot.status.mode,
        snapshot.status.connection_count,
        snapshot.status.config_hash,
        snapshot.status.deploy_revision.as_deref().unwrap_or("-"),
        format_timestamp(snapshot.status.started_at),
        snapshot.status.connection_id,
    ));
    lines.push(format!(
        "Fetched: {}",
        format_timestamp(snapshot.fetched_at)
    ));
    lines.push(String::new());
    lines.push("Connections".to_string());
    for connection in &snapshot.connections {
        lines.push(format!(
            "{:<24} {:<8} {:<10} {:<10} {:<14} {:<18} age={:<8} status={} last_error={}",
            connection.id,
            if connection.enabled {
                "enabled"
            } else {
                "disabled"
            },
            connection.source_kind,
            connection.destination_kind,
            connection.phase,
            connection.reason_code,
            format_age(connection.max_checkpoint_age_seconds),
            connection.last_sync_status.as_deref().unwrap_or("-"),
            connection.last_error.as_deref().unwrap_or("-"),
        ));
        lines.push(format!(
            "  started={} finished={}",
            connection
                .last_sync_started_at
                .map(format_timestamp)
                .unwrap_or_else(|| "-".to_string()),
            connection
                .last_sync_finished_at
                .map(format_timestamp)
                .unwrap_or_else(|| "-".to_string()),
        ));
    }

    if let Some(progress) = &snapshot.progress {
        lines.push(String::new());
        lines.push(format!("Selected: {}", progress.connection_id));
        lines.push(format!(
            "Runtime: connection={} phase={} reason={} age={} finished={} error={}",
            progress.runtime.connection_id,
            progress.runtime.phase,
            progress.runtime.reason_code,
            format_age(progress.runtime.max_checkpoint_age_seconds),
            progress
                .runtime
                .last_sync_finished_at
                .map(format_timestamp)
                .unwrap_or_else(|| "-".to_string()),
            progress.runtime.last_error.as_deref().unwrap_or("-"),
        ));
        if let Some(run) = &progress.current_run {
            lines.push(format!(
                "Run: {} connection={} status={} rows_read={} rows_written={} rows_upserted={} rate_limit_hits={} error={}",
                run.run_id,
                run.connection_id,
                run.status.as_deref().unwrap_or("-"),
                run.rows_read,
                run.rows_written,
                run.rows_upserted,
                run.rate_limit_hits,
                run.error.as_deref().unwrap_or("-"),
            ));
        }
        lines.push("Tables".to_string());
        for table in &progress.tables {
            lines.push(format!(
                "{:<42} {:<13} reason={:<18} age={:<8} lag={:<8} chunks={:<7} last_pk={} stats={}",
                table.table_name,
                table.phase,
                table.reason_code,
                format_age(table.checkpoint_age_seconds),
                format_age(table.lag_seconds),
                if table.snapshot_chunks_total > 0 {
                    format!(
                        "{}/{}",
                        table.snapshot_chunks_complete, table.snapshot_chunks_total
                    )
                } else {
                    "-".to_string()
                },
                table
                    .checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.last_primary_key.as_deref())
                    .unwrap_or("-"),
                table
                    .stats
                    .as_ref()
                    .map(|stats| format!(
                        "read={} written={} upserted={} deleted={}",
                        stats.rows_read,
                        stats.rows_written,
                        stats.rows_upserted,
                        stats.rows_deleted
                    ))
                    .unwrap_or_else(|| "-".to_string()),
            ));
        }
    }

    format!("{}\n", lines.join("\n"))
}

fn format_age(age_seconds: Option<i64>) -> String {
    match age_seconds {
        Some(seconds) if seconds < 60 => format!("{seconds}s"),
        Some(seconds) if seconds < 3600 => format!("{}m", seconds / 60),
        Some(seconds) => format!("{}h", seconds / 3600),
        None => "-".to_string(),
    }
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S %Z")
        .to_string()
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalSession {
    fn new() -> Result<Self> {
        enable_raw_mode().context("enabling raw mode")?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, Hide).context("entering alternate screen")?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).context("creating terminal")?;
        Ok(Self { terminal })
    }

    fn draw<F>(&mut self, render: F) -> Result<()>
    where
        F: FnOnce(&mut Frame<'_>),
    {
        self.terminal.draw(render).context("drawing monitor UI")?;
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), Show, LeaveAlternateScreen);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    const TEST_PRIVATE_KEY_PEM: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCnqmojl7pWw+Vq
/zX8CpkQLCpOgvT8JuChGB5d15nl7pTt8jeYH+yCznxYu5nyOo1OXqYAhRzKaQ01
IQXgrOo4GFKIPc+7XWThFF62Ay3jIYyeeauyD/s2rfIYDheJTvLPl+E7cbvX8hxy
DxSemUNd9Mn38PiYILzb1s3zOn830rkTSD7iPpYsx9ItvCxpWmq2euBfjflC6voE
CxbLvBMK/Z8e4eXvlRuEEPrKDnuB2w3CLdjr3klglT8XHhkRORRMSQRGKvJg/Jir
GSEdGTMw+VIMBAKSVla4WjbyZGXVesA4KOP+7kfKqvAJGXALKd+JycngFHPpA6sU
1aOgV2RLAgMBAAECggEAGNuqvdkuftOvbWQmKFaX5+5sXVSMJuBKuIefZPFkt1Le
kMKzHGJLSf98LxmtUtz8e0yMFxKlOJtHooNhYDSyyxtMDTgA1vobTUWcXybshDrC
ovJOEunMqIg0lv1r3ucuF7ogYhRUMcmLDxwORg9aDhGPaiu3Z7Ke3Yck5LVdDDT7
nHVFv436NVZ+n+x3cVhsRIhblLACaHbfP9Qb96bG9acsQZbbH72stV4kWSt1CwBi
yJFo2v+h1AHn7AUrxikFulUACf0z5BJ600ISotkWyyVHc+gM146Yyc5ZeRL6Tnmq
PACxMsLXCqcYh45RLu6pBpAmP4sSe5Xah7bjdlzwLQKBgQDi7b6V8+BhCy0kF513
FtbIV7Zwji8kDcl1G+JHD3MHt4YFu/PNqbEma2j0oak7acMw6oxNoFEhXFCg/3y+
t71+wY/DTFRYX8iL/xeRryiDZvD12NV3j5mH6fK8Oiu4sxMXwolusUC+8pyFuWI/
eIV4h9zsf0HWN+udHrN+Zu72tQKBgQC9JRsnyHLlaQng1S9+q6mfoZswCz02LZn8
IngKxaiMiINkTbsiXIBeCPh+vVBCaepd/wcdEzwyXgn/Wp1rYyTumS6eoFuT5eyq
SUKt9FfYKOE0vZGqtAcVfAbiWBiMov3sGc5FfCchf5EGgVS2NgluSVxAW8uRRzkd
7QhcOuLO/wKBgQCzRdqwoA982tV4k+dkM3jOoOySEuGO/A1RJQwn0z6us/9+/DLp
IMvAbE5oJGaLd0wqksDwelxdnI5eAjhMet+LCeNHCEAB6PmID6hRAS1iUaq+reRG
Jf3Gb73BkbsEmQPWW2szNXjO4N9ijUfemJno1HxloUsjrt3GLIDktPDHmQKBgA8E
KiK/bDfAXhNmeW3SDRZqSxrGWaa6ehYlWmhohtgZYm0NKsUwmNReW/Qb7YpIRF4Q
CC2LwGSzSJHoTMUgyubSbHwVeQ/F2kMuq8eJtYuouzBnuG/X+RQAk79WhSRtMEGV
TuX/VE/5g7cDf4kzww3pbxSA9SlkgSlaDybbWfRbAoGBAK/vWfHGKpCxUKefbwnW
n39PxGMwuG94QQd8sRRxkwgzs8phKwLXBwTzN8cZ1vLjrjQaHiRAarJ6Nl28lfTP
BJDvfWrriikStbF6cX5uEWyUgvIZBjtd+Q+c7IRAm2zl/loZ+nYbk4+G13yTXkjb
RZZXUhlVF0i1Uviw8GQfe9su
-----END PRIVATE KEY-----";
    const TEST_PUBLIC_KEY_PEM: &str = r"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp6pqI5e6VsPlav81/AqZ
ECwqToL0/CbgoRgeXdeZ5e6U7fI3mB/sgs58WLuZ8jqNTl6mAIUcymkNNSEF4Kzq
OBhSiD3Pu11k4RRetgMt4yGMnnmrsg/7Nq3yGA4XiU7yz5fhO3G71/Iccg8UnplD
XfTJ9/D4mCC829bN8zp/N9K5E0g+4j6WLMfSLbwsaVpqtnrgX435Qur6BAsWy7wT
Cv2fHuHl75UbhBD6yg57gdsNwi3Y695JYJU/Fx4ZETkUTEkERiryYPyYqxkhHRkz
MPlSDAQCklZWuFo28mRl1XrAOCjj/u5HyqrwCRlwCynficnJ4BRz6QOrFNWjoFdk
SwIDAQAB
-----END PUBLIC KEY-----";

    fn test_connections() -> Vec<ConnectionSummary> {
        vec![
            ConnectionSummary {
                id: "app".to_string(),
                enabled: true,
                source_kind: "postgres".to_string(),
                destination_kind: "bigquery".to_string(),
                last_sync_started_at: None,
                last_sync_finished_at: None,
                last_sync_status: Some("running".to_string()),
                last_error: None,
                phase: "running".to_string(),
                reason_code: "cdc_following".to_string(),
                max_checkpoint_age_seconds: Some(12),
            },
            ConnectionSummary {
                id: "salesforce".to_string(),
                enabled: true,
                source_kind: "salesforce".to_string(),
                destination_kind: "bigquery".to_string(),
                last_sync_started_at: None,
                last_sync_finished_at: None,
                last_sync_status: Some("success".to_string()),
                last_error: None,
                phase: "healthy".to_string(),
                reason_code: "healthy".to_string(),
                max_checkpoint_age_seconds: Some(44),
            },
        ]
    }

    #[test]
    fn resolve_selected_connection_prefers_requested_when_present() {
        let connections = test_connections();
        assert_eq!(
            resolve_selected_connection_id(&connections, Some("salesforce"))
                .expect("selected connection")
                .as_deref(),
            Some("salesforce")
        );
    }

    #[test]
    fn resolve_selected_connection_errors_for_unknown_requested_connection() {
        let connections = test_connections();
        let err = resolve_selected_connection_id(&connections, Some("missing"))
            .expect_err("missing connection should error");
        assert!(err.to_string().contains("connection not found"));
    }

    #[test]
    fn reconcile_selection_respects_runtime_selection_without_requested_connection() {
        let snapshot = MonitorSnapshot {
            status: StatusResponse {
                service: "cdsync".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                started_at: Utc::now(),
                mode: "run".to_string(),
                connection_id: "all".to_string(),
                connection_count: 2,
                config_hash: "abc123".to_string(),
                deploy_revision: Some("rev1".to_string()),
                last_restart_reason: "startup".to_string(),
            },
            connections: test_connections(),
            selected_connection_id: Some("salesforce".to_string()),
            progress: None,
            fetched_at: Utc::now(),
        };
        let mut view_state = MonitorViewState { selected_index: 0 };

        reconcile_selection(&mut view_state, &snapshot, None);

        assert_eq!(view_state.selected_index, 1);
    }

    #[test]
    fn main_layout_allocates_space_for_bordered_header_and_footer() {
        let layout = main_layout(Rect::new(0, 0, 120, 40));
        assert_eq!(layout[0].height, 5);
        assert_eq!(layout[2].height, 3);
    }

    #[test]
    fn render_plain_includes_selected_connection_details() {
        let snapshot = MonitorSnapshot {
            status: StatusResponse {
                service: "cdsync".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                started_at: Utc::now(),
                mode: "run".to_string(),
                connection_id: "all".to_string(),
                connection_count: 2,
                config_hash: "abc123".to_string(),
                deploy_revision: Some("rev1".to_string()),
                last_restart_reason: "startup".to_string(),
            },
            connections: test_connections(),
            selected_connection_id: Some("app".to_string()),
            progress: Some(ProgressResponse {
                connection_id: "app".to_string(),
                runtime: ConnectionRuntime {
                    connection_id: "app".to_string(),
                    phase: "running".to_string(),
                    reason_code: "cdc_following".to_string(),
                    last_sync_started_at: None,
                    last_sync_finished_at: None,
                    last_sync_status: Some("running".to_string()),
                    last_error: None,
                    max_checkpoint_age_seconds: Some(12),
                    config_hash: "abc123".to_string(),
                    deploy_revision: Some("rev1".to_string()),
                    last_restart_reason: "startup".to_string(),
                },
                current_run: None,
                tables: vec![TableProgress {
                    table_name: "public.accounts".to_string(),
                    phase: "healthy".to_string(),
                    reason_code: "healthy".to_string(),
                    checkpoint_age_seconds: Some(4),
                    lag_seconds: Some(4),
                    snapshot_chunks_total: 0,
                    snapshot_chunks_complete: 0,
                    checkpoint: Some(TableCheckpointSummary {
                        last_primary_key: Some("42".to_string()),
                    }),
                    stats: None,
                }],
            }),
            fetched_at: Utc::now(),
        };

        let rendered = render_plain(&snapshot);
        assert!(rendered.contains("Connections"));
        assert!(rendered.contains("Selected: app"));
        assert!(rendered.contains("public.accounts"));
        assert!(rendered.contains("last_pk=42"));
    }

    #[test]
    fn monitor_auth_config_mints_expected_token_claims() {
        let _ = jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.install_default();
        let key_path = std::env::temp_dir().join(format!(
            "cdsync-monitor-key-{}.pem",
            Uuid::new_v4().simple()
        ));
        fs::write(&key_path, TEST_PRIVATE_KEY_PEM).expect("write private key");

        let auth = MonitorAuthConfig::from_args(&MonitorAuthArgs {
            private_key_path: Some(key_path.display().to_string()),
            issuer: Some("ops-cli".to_string()),
            audience: Some("cdsync".to_string()),
            subject: Some("ops-cli:monitor".to_string()),
            scope: "cdsync:admin".to_string(),
            kid: Some("ops-cli-20260401".to_string()),
            expires_in_seconds: 300,
        })
        .expect("auth config");

        let token = auth.mint_token().expect("token");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&["cdsync"]);
        let decoded = decode::<MonitorServiceJwtClaims>(
            &token,
            &DecodingKey::from_rsa_pem(TEST_PUBLIC_KEY_PEM.as_bytes()).expect("decoding key"),
            &validation,
        )
        .expect("decode token");

        assert_eq!(decoded.claims.iss, "ops-cli");
        assert_eq!(decoded.claims.aud, "cdsync");
        assert_eq!(decoded.claims.sub, "ops-cli:monitor");
        assert_eq!(decoded.claims.scope, "cdsync:admin");
        assert_eq!(decoded.header.kid.as_deref(), Some("ops-cli-20260401"));
    }

    #[test]
    fn monitor_auth_config_honors_issuer_and_audience_env_overrides() {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock");

        let key_path = std::env::temp_dir().join(format!(
            "cdsync-monitor-env-key-{}.pem",
            Uuid::new_v4().simple()
        ));
        fs::write(&key_path, TEST_PRIVATE_KEY_PEM).expect("write private key");

        temp_env::with_vars(
            [
                (MONITOR_ISSUER_ENV, Some("env-ops")),
                (MONITOR_AUDIENCE_ENV, Some("env-cdsync")),
                (MONITOR_KID_ENV, None),
            ],
            || {
                let auth = MonitorAuthConfig::from_args(&MonitorAuthArgs {
                    private_key_path: Some(key_path.display().to_string()),
                    issuer: None,
                    audience: None,
                    subject: None,
                    scope: "cdsync:admin".to_string(),
                    kid: None,
                    expires_in_seconds: 300,
                })
                .expect("auth config from env");

                assert_eq!(auth.issuer, "env-ops");
                assert_eq!(auth.audience, "env-cdsync");
                assert_eq!(auth.kid, "env-ops-20260401");
                assert_eq!(auth.subject, "env-ops:monitor");
            },
        );
    }
}
