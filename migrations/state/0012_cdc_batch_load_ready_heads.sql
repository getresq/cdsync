create table if not exists cdc_batch_load_ready_heads (
    connection_id text not null,
    table_key text not null,
    head_job_id text not null,
    first_sequence bigint not null,
    created_at bigint not null,
    updated_at bigint not null,
    primary key (connection_id, table_key)
);

insert into cdc_batch_load_ready_heads (
    connection_id,
    table_key,
    head_job_id,
    first_sequence,
    created_at,
    updated_at
)
select connection_id,
       table_key,
       job_id as head_job_id,
       first_sequence,
       created_at,
       updated_at
from (
    select distinct on (connection_id, table_key)
           connection_id,
           table_key,
           job_id,
           first_sequence,
           created_at,
           updated_at
    from cdc_batch_load_jobs
    where status in ('pending', 'running', 'failed')
    order by connection_id, table_key, first_sequence, created_at
) heads
on conflict (connection_id, table_key) do update set
    head_job_id = excluded.head_job_id,
    first_sequence = excluded.first_sequence,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at;

create index if not exists cdc_batch_load_ready_heads_connection_sequence_idx
    on cdc_batch_load_ready_heads (connection_id, first_sequence, created_at);
