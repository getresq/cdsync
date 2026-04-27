alter table cdc_commit_fragments
    add column if not exists expected_fragments bigint;

create table if not exists cdc_commit_sequences (
    connection_id text not null,
    sequence bigint not null,
    commit_lsn text not null,
    expected_fragments bigint not null,
    created_at bigint not null,
    updated_at bigint not null,
    primary key (connection_id, sequence)
);

alter table cdc_commit_fragments
    alter column expected_fragments set default 1;

create index if not exists cdc_commit_sequences_connection_sequence_idx
    on cdc_commit_sequences (connection_id, sequence);

create index if not exists cdc_commit_fragments_connection_sequence_expected_idx
    on cdc_commit_fragments (connection_id, sequence, expected_fragments);
