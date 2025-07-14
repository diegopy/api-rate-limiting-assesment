// @generated automatically by Diesel CLI.

diesel::table! {
    transaction_queue (id) {
        id -> Uuid,
        account_id -> Text,
        transaction_data -> Jsonb,
        status -> Text,
        priority -> Int4,
        retry_count -> Int4,
        max_retries -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
        scheduled_at -> Nullable<Timestamptz>,
        processed_at -> Nullable<Timestamptz>,
        error_message -> Nullable<Text>,
    }
}

diesel::table! {
    rate_limits (id) {
        id -> Uuid,
        limit_type -> Text,
        max_requests -> Int4,
        window_seconds -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    accounts (id) {
        id -> Text,
        limit_type -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}