use postgres_models::DbPool;
use redis_cache::RedisPool;
use std::fmt::Debug;

#[derive(Clone)]
pub struct AppState {
    pub db_pool: DbPool,
    pub redis_pool: RedisPool,
}

impl AppState {
    pub async fn new(database_url: &str, redis_url: &str) -> anyhow::Result<Self> {
        let db_pool = postgres_models::create_pool(database_url).await
            .map_err(|e| anyhow::anyhow!("Failed to create database pool: {}", e))?;
        let redis_pool = redis_cache::create_pool(redis_url).await
            .map_err(|e| anyhow::anyhow!("Failed to create Redis pool: {}", e))?;

        Ok(Self {
            db_pool,
            redis_pool,
        })
    }
}
