alter table cdc_batch_load_jobs
    add column if not exists reducer_eligible boolean not null default false;

update cdc_batch_load_jobs
set reducer_eligible = (
    barrier_kind is null
    and payload_json::jsonb ? 'staging_schema'
    and payload_json::jsonb -> 'staging_schema' <> 'null'::jsonb
)
where reducer_eligible is false;

create index if not exists cdc_batch_load_jobs_connection_ready_reducer_idx
    on cdc_batch_load_jobs (
        connection_id,
        status,
        stage,
        reducer_eligible,
        first_sequence,
        created_at
    );
