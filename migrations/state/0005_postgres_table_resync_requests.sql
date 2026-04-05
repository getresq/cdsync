create table if not exists postgres_table_resync_requests (
    connection_id text not null,
    source_table text not null,
    requested_at bigint not null,
    primary key (connection_id, source_table)
);

create index if not exists postgres_table_resync_requests_connection_requested_idx
    on postgres_table_resync_requests (connection_id, requested_at);
