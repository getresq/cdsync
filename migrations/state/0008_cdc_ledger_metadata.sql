alter table cdc_batch_load_jobs
    add column if not exists stage text;

alter table cdc_batch_load_jobs
    add column if not exists staging_table text;

alter table cdc_batch_load_jobs
    add column if not exists artifact_uri text;

alter table cdc_batch_load_jobs
    add column if not exists load_job_id text;

alter table cdc_batch_load_jobs
    add column if not exists merge_job_id text;

alter table cdc_batch_load_jobs
    add column if not exists primary_key_lane text;

alter table cdc_batch_load_jobs
    add column if not exists barrier_kind text;

alter table cdc_batch_load_jobs
    add column if not exists ledger_metadata_json text;

update cdc_batch_load_jobs
set stage = case status
    when 'pending' then 'received'
    when 'running' then 'applying'
    when 'succeeded' then 'applied'
    when 'failed' then 'failed'
    else 'received'
end
where stage is null;

create index if not exists cdc_batch_load_jobs_connection_stage_sequence_idx
    on cdc_batch_load_jobs (connection_id, stage, first_sequence, created_at);

create index if not exists cdc_batch_load_jobs_connection_table_lane_sequence_idx
    on cdc_batch_load_jobs (connection_id, table_key, primary_key_lane, first_sequence, created_at);

alter table cdc_commit_fragments
    add column if not exists stage text;

alter table cdc_commit_fragments
    add column if not exists artifact_uri text;

alter table cdc_commit_fragments
    add column if not exists staging_table text;

alter table cdc_commit_fragments
    add column if not exists load_job_id text;

alter table cdc_commit_fragments
    add column if not exists merge_job_id text;

alter table cdc_commit_fragments
    add column if not exists primary_key_lane text;

alter table cdc_commit_fragments
    add column if not exists barrier_kind text;

alter table cdc_commit_fragments
    add column if not exists ledger_metadata_json text;

update cdc_commit_fragments
set stage = case status
    when 'pending' then 'received'
    when 'succeeded' then 'applied'
    when 'failed' then 'failed'
    else 'received'
end
where stage is null;

create index if not exists cdc_commit_fragments_connection_stage_sequence_idx
    on cdc_commit_fragments (connection_id, stage, sequence, created_at);

create index if not exists cdc_commit_fragments_connection_table_lane_sequence_idx
    on cdc_commit_fragments (connection_id, table_key, primary_key_lane, sequence, created_at);
