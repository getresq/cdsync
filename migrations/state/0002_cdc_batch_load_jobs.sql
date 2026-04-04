create table if not exists cdc_batch_load_jobs (
    connection_id text not null,
    job_id text primary key,
    table_key text not null,
    first_sequence bigint not null,
    status text not null,
    payload_json text not null,
    attempt_count integer not null default 0,
    last_error text,
    created_at bigint not null,
    updated_at bigint not null
);

create index if not exists cdc_batch_load_jobs_connection_status_sequence_idx
    on cdc_batch_load_jobs (connection_id, status, first_sequence, created_at);

create index if not exists cdc_batch_load_jobs_connection_table_sequence_idx
    on cdc_batch_load_jobs (connection_id, table_key, first_sequence, created_at);
