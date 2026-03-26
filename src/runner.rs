use crate::config::ConnectionConfig;
use anyhow::Context;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Clone)]
pub struct ShutdownSignal {
    rx: watch::Receiver<bool>,
}

#[derive(Clone)]
pub struct ShutdownController {
    tx: watch::Sender<bool>,
}

impl ShutdownController {
    pub fn new() -> (Self, ShutdownSignal) {
        let (tx, rx) = watch::channel(false);
        (Self { tx }, ShutdownSignal { rx })
    }

    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

impl ShutdownSignal {
    pub fn is_shutdown(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn changed(&mut self) -> bool {
        if self.is_shutdown() {
            return true;
        }
        self.rx.changed().await.is_ok() && *self.rx.borrow()
    }
}

pub fn schedule_interval(connection: &ConnectionConfig) -> anyhow::Result<Duration> {
    let raw = connection
        .schedule
        .as_ref()
        .and_then(|schedule| schedule.every.as_deref())
        .context("run mode requires connection.schedule.every for non-CDC connections")?;
    parse_schedule_interval(raw)
}

pub fn parse_schedule_interval(raw: &str) -> anyhow::Result<Duration> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("schedule.every must not be empty");
    }
    let (digits, unit) = match value.chars().last() {
        Some(last) if last.is_ascii_alphabetic() => {
            let (digits, unit) = value.split_at(value.len().saturating_sub(1));
            if !digits.chars().all(|ch| ch.is_ascii_digit()) {
                anyhow::bail!("invalid schedule.every value `{}`", raw);
            }
            (digits, unit)
        }
        Some(_) if value.chars().all(|ch| ch.is_ascii_digit()) => (value, "s"),
        _ => anyhow::bail!("invalid schedule.every value `{}`", raw),
    };
    let quantity = digits
        .parse::<u64>()
        .with_context(|| format!("invalid schedule.every value `{}`", raw))?;
    if quantity == 0 {
        anyhow::bail!("schedule.every must be greater than zero");
    }
    let seconds = match unit {
        "s" => quantity,
        "m" => quantity.saturating_mul(60),
        "h" => quantity.saturating_mul(60 * 60),
        "d" => quantity.saturating_mul(60 * 60 * 24),
        _ => anyhow::bail!("unsupported schedule.every unit `{}`", unit),
    };
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_schedule_interval_supports_common_units() {
        assert_eq!(
            parse_schedule_interval("30s").expect("seconds"),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_schedule_interval("5m").expect("minutes"),
            Duration::from_secs(300)
        );
        assert_eq!(
            parse_schedule_interval("2h").expect("hours"),
            Duration::from_secs(7200)
        );
        assert_eq!(
            parse_schedule_interval("1d").expect("days"),
            Duration::from_secs(86400)
        );
        assert_eq!(
            parse_schedule_interval("15").expect("default seconds"),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn parse_schedule_interval_rejects_invalid_values() {
        assert!(parse_schedule_interval("").is_err());
        assert!(parse_schedule_interval("0s").is_err());
        assert!(parse_schedule_interval("5w").is_err());
        assert!(parse_schedule_interval("abc").is_err());
    }
}
