create table if not exists cdc_feedback_state (
    connection_id text primary key,
    next_sequence_to_ack bigint not null default 0,
    last_received_lsn text,
    last_flushed_lsn text,
    last_persisted_lsn text,
    last_status_update_sent_at bigint,
    last_keepalive_reply_at bigint,
    last_slot_feedback_lsn text,
    updated_at bigint not null
);

insert into cdc_feedback_state (
    connection_id,
    next_sequence_to_ack,
    last_received_lsn,
    last_flushed_lsn,
    last_persisted_lsn,
    last_status_update_sent_at,
    last_keepalive_reply_at,
    last_slot_feedback_lsn,
    updated_at
)
select
    connection_id,
    next_sequence_to_ack,
    last_received_lsn,
    last_flushed_lsn,
    last_persisted_lsn,
    last_status_update_sent_at,
    last_keepalive_reply_at,
    last_slot_feedback_lsn,
    updated_at
from cdc_watermark_state
on conflict (connection_id) do nothing;
