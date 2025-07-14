use deadpool_redis::{redis::AsyncCommands, Config, Pool, Runtime};
use tracing::info;

pub type RedisPool = Pool;
pub type RedisConnection = deadpool_redis::Connection;

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("Redis pool error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),
    
    #[error("Redis error: {0}")]
    Redis(#[from] deadpool_redis::redis::RedisError),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Configuration error: {0}")]
    Config(String),
}

pub async fn create_pool(redis_url: &str) -> Result<RedisPool, RedisError> {
    let cfg = Config::from_url(redis_url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))
        .map_err(|e| RedisError::Config(e.to_string()))?;
    Ok(pool)
}

pub struct RateLimiter {
    pool: RedisPool,
}

impl RateLimiter {
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    pub async fn check_rate_limit(
        &self,
        key: &str,
        max_requests: u32,
        window_seconds: u64,
    ) -> Result<RateLimitResult, RedisError> {
        let mut conn = self.pool.get().await?;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let window_start = current_time - (window_seconds * 1000);
        let window_start_nanos = (window_start * 1_000_000) as f64;
        let rate_limit_key = format!("rate_limit:{}", key);
        
        // Remove old entries from sorted set
        let _: i32 = deadpool_redis::redis::cmd("ZREMRANGEBYSCORE")
            .arg(&rate_limit_key)
            .arg(0.0)
            .arg(window_start_nanos)
            .query_async(&mut *conn)
            .await?;
        
        // Add new request first with unique score to handle concurrent requests
        // Use nanoseconds instead of milliseconds for better uniqueness
        let current_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as f64;
        let _: i32 = conn.zadd(&rate_limit_key, current_nanos, current_nanos).await?;
        
        // Count current requests in window (including the one we just added)
        // Use the current nanos time that we just added to ensure consistency
        let count: i32 = conn.zcount(&rate_limit_key, window_start_nanos, current_nanos).await?;
        
        if count > max_requests as i32 {
            return Ok(RateLimitResult {
                allowed: false,
                remaining: 0,
                reset_at: (current_time + (window_seconds * 1000)) / 1000,
            });
        }
        let _: bool = conn.expire(&rate_limit_key, window_seconds as i64).await?;
        
        Ok(RateLimitResult {
            allowed: true,
            remaining: (max_requests as i32 - count).max(0) as u32,
            reset_at: (current_time + (window_seconds * 1000)) / 1000,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitResult {
    pub allowed: bool,
    pub remaining: u32,
    pub reset_at: u64,
}

pub struct QueueManager {
    pool: RedisPool,
}

impl QueueManager {
    pub fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(&self, queue_name: &str, data: &str) -> Result<i64, RedisError> {
        let mut conn = self.pool.get().await?;
        let position: i64 = conn.rpush(queue_name, data).await?;
        Ok(position)
    }

    /// Enqueue with priority - higher priority number = processed first
    pub async fn enqueue_with_priority(&self, queue_name: &str, data: &str, priority: i32) -> Result<i64, RedisError> {
        let mut conn = self.pool.get().await?;
        let priority_queue_name = format!("{}_priority", queue_name);
        
        // Use timestamp in nanoseconds for tie-breaking (FIFO within same priority)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as f64;
        
        // Score calculation: higher priority = lower score (processed first)
        // Use timestamp for tie-breaking within same priority level
        let score = (1000 - priority) as f64 + (timestamp / 1e15); // Timestamp scaled to avoid affecting priority
        
        // Add to priority queue (sorted set)
        let _: i32 = conn.zadd(&priority_queue_name, data, score).await?;
        
        // Get current position in priority order
        let position = self.get_priority_position(&priority_queue_name, data).await?;
        Ok(position)
    }

    /// Get position in priority queue (1-indexed)
    async fn get_priority_position(&self, priority_queue_name: &str, data: &str) -> Result<i64, RedisError> {
        let mut conn = self.pool.get().await?;
        
        // Get rank (0-indexed) and convert to 1-indexed position
        let rank: Option<i64> = conn.zrank(priority_queue_name, data).await?;
        match rank {
            Some(r) => Ok(r + 1),
            None => Ok(1), // Fallback if not found
        }
    }

    /// Get total count of items in priority queue
    pub async fn priority_queue_length(&self, queue_name: &str) -> Result<i64, RedisError> {
        let mut conn = self.pool.get().await?;
        let priority_queue_name = format!("{}_priority", queue_name);
        let length: i64 = conn.zcard(&priority_queue_name).await?;
        Ok(length)
    }

    /// Dequeue next item by priority (highest priority first)
    pub async fn dequeue_by_priority(&self, queue_name: &str) -> Result<Option<String>, RedisError> {
        let mut conn = self.pool.get().await?;
        let priority_queue_name = format!("{}_priority", queue_name);
        
        // Pop item with lowest score (highest priority)
        let result: Vec<String> = conn.zpopmin(&priority_queue_name, 1).await?;
        
        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result[0].clone()))
        }
    }

    /// Get queue contents in priority order for testing
    pub async fn get_priority_queue_order(&self, queue_name: &str) -> Result<Vec<String>, RedisError> {
        let mut conn = self.pool.get().await?;
        let priority_queue_name = format!("{}_priority", queue_name);
        
        // Get all items in score order (ascending = highest priority first)
        let items: Vec<String> = conn.zrange(&priority_queue_name, 0, -1).await?;
        Ok(items)
    }

    pub async fn dequeue(&self, queue_name: &str) -> Result<Option<String>, RedisError> {
        let mut conn = self.pool.get().await?;
        let result: Option<String> = conn.lpop(queue_name, None).await?;
        Ok(result)
    }

    pub async fn queue_length(&self, queue_name: &str) -> Result<i64, RedisError> {
        let mut conn = self.pool.get().await?;
        let length: i64 = conn.llen(queue_name).await?;
        Ok(length)
    }

    pub async fn get_queue_position(&self, queue_name: &str, data: &str) -> Result<Option<i64>, RedisError> {
        let mut conn = self.pool.get().await?;
        let items: Vec<String> = conn.lrange(queue_name, 0, -1).await?;
        
        for (index, item) in items.iter().enumerate() {
            if item == data {
                return Ok(Some(index as i64 + 1));
            }
        }
        
        Ok(None)
    }
}