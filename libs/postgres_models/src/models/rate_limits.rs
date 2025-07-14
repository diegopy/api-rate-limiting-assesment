use crate::schema::rate_limits;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
#[diesel(table_name = rate_limits)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct RateLimit {
    pub id: Uuid,
    pub limit_type: String,
    #[diesel(deserialize_as = i32)]
    pub max_requests: u32,
    #[diesel(deserialize_as = i32)]
    pub window_seconds: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Insertable)]
#[diesel(table_name = rate_limits)]
pub struct NewRateLimit {
    pub id: Uuid,
    pub limit_type: String,
    pub max_requests: i32,
    pub window_seconds: i32,
}

impl NewRateLimit {
    pub fn new(limit_type: String, max_requests: i32, window_seconds: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            limit_type,
            max_requests,
            window_seconds,
        }
    }
}