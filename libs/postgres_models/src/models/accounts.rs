use crate::schema::accounts;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Queryable, Selectable)]
#[diesel(table_name = accounts)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Account {
    pub id: String,
    pub limit_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Insertable)]
#[diesel(table_name = accounts)]
pub struct NewAccount {
    pub id: String,
    pub limit_type: String,
}

impl NewAccount {
    pub fn new(id: String, limit_type: String) -> Self {
        Self {
            id,
            limit_type,
        }
    }
}