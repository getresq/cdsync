alter table connection_state
    add column if not exists dynamodb_follow_state_json text;
