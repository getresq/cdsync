alter table if exists cdc_watermark_state
    add column if not exists last_relevant_change_seen_at bigint;

alter table if exists cdc_watermark_state
    add column if not exists last_status_update_sent_at bigint;

alter table if exists cdc_watermark_state
    add column if not exists last_keepalive_reply_at bigint;

alter table if exists cdc_watermark_state
    add column if not exists last_slot_feedback_lsn text;
