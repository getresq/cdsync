alter table connection_state
    add column if not exists last_error_reason text;

update connection_state
set last_error_reason = case
    when last_error is null then null
    when last_error ilike '%schema change detected%'
      or last_error ilike '%incompatible schema change detected%' then 'schema_blocked'
    when last_error ilike '%publication%' then 'publication_blocked'
    when last_error ilike '%snapshot blocked for table(s):%' then 'snapshot_blocked'
    when last_error ilike '%already locked%' then 'lock_contention'
    else 'last_run_failed'
end
where last_error_reason is null;

alter table cdc_batch_load_jobs
    add column if not exists retry_class text;

update cdc_batch_load_jobs
set retry_class = case
    when last_error is null then null
    when last_error ilike '%quota exceeded%'
      and last_error ilike '%dml jobs writing to a table%' then 'backpressure'
    when last_error ilike '%failed to process CDC batch-load job%'
      or last_error ilike '%merging staging BigQuery table%'
      or last_error ilike '%BigQuery query returned errors%' then 'transient'
    else 'permanent'
end
where retry_class is null;
