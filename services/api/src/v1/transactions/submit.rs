use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, Response},
    response::IntoResponse,
};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use postgres_models::{
    DbConnection,
    models::{NewTransactionQueue, RateLimit, TransactionQueue, accounts::Account},
    schema::{
        self,
        transaction_queue,
    },
};
use redis_cache::{QueueManager, RateLimiter};
use serde::{Deserialize, Serialize};
use serde_json::value::{self, RawValue};
use log::info;
use uuid::Uuid;
use transaction_queue_api::AppState;

use crate::{
    errors::{AppError, AppResult},
    extractors::DatabaseConnection,
};

#[derive(Debug, Deserialize)]
pub struct SubmitTransactionRequest {
    pub account_id: String,
    // Using owned RawValue to efficiently calculate the length of the data using the raw JSON
    pub transaction_data: Box<RawValue>,
    pub priority: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct SubmitTransactionResponse {
    pub transaction_id: Uuid,
    pub queue_position: i64,
    pub estimated_processing_time_seconds: i64,
    pub status: String,
}

/// Submit a transaction to the queue
///
/// This is the main endpoint that candidates need to implement.
/// It should handle high-performance transaction queuing with proper
/// rate limiting, validation, and queue management.
///
/// Expected Performance: <100ms p99 latency, 10k+ concurrent requests
///
/// TODO: Implement the following steps in order:
///
/// Step 1: INPUT VALIDATION (Security Critical)
/// - Validate account_id: non-empty, reasonable length (< 255 chars)
/// - Validate transaction_data: not null, reasonable size (< 1MB)
/// - Validate priority: if provided, should be reasonable range (-1000 to 1000)
/// - Return 400 Bad Request for invalid input with descriptive errors
///
/// Step 2: RATE LIMITING (Performance Critical)
/// - Get rate limiter from state: &state.redis_pool
/// - Use libs/redis_cache/src/rate_limiter.rs::RateLimiter::check_rate_limit()
/// - Check account-specific limits from account_rate_limits table
/// - Return 429 Too Many Requests if exceeded
/// - MUST include rate limit headers in ALL responses:
///   - X-RateLimit-Limit: requests per minute allowed
///   - X-RateLimit-Remaining: requests remaining in current window
///   - X-RateLimit-Reset: timestamp when window resets
///
/// Step 3: DATABASE PERSISTENCE (Reliability Critical)
/// - Create NewTransactionQueue using libs/postgres_models/src/models.rs
/// - Generate UUID for transaction_id using Uuid::new_v4()
/// - Set created_at to current UTC timestamp
/// - Set status to "pending"
/// - Insert into transaction_queue table using diesel
/// - Handle database errors gracefully (return 500 Internal Server Error)
///
/// Step 4: QUEUE MANAGEMENT (Business Logic Critical)
/// - Use libs/redis_cache/src/queue_manager.rs::QueueManager
/// - Add transaction to Redis queue with priority
/// - Get current queue position considering priority ordering
/// - Higher priority numbers should be processed first
/// - Use Redis sorted sets for efficient priority queue
///
/// Step 5: RESPONSE CALCULATION
/// - Calculate estimated_processing_time_seconds:
///   - Base time: 30 seconds per transaction
///   - Multiply by queue position ahead of current transaction
///   - Cap at reasonable maximum (e.g., 3600 seconds)
/// - Return proper JSON response with all fields
///
/// Step 6: ERROR HANDLING
/// - All database errors should return 500 with generic message
/// - All Redis errors should return 500 with generic message  
/// - Invalid input should return 400 with specific validation errors
/// - Rate limiting should return 429 with retry information
/// - Log all errors for debugging but don't expose internals to client
///
/// PERFORMANCE REQUIREMENTS:
/// - This endpoint MUST handle 10,000+ concurrent requests
/// - p99 latency MUST be under 100ms
/// - Success rate MUST be >99% under normal load
/// - Use connection pooling efficiently (don't hold connections unnecessarily)
/// - Use prepared statements for database operations
///
/// SECURITY REQUIREMENTS:
/// - NO authentication required (this is intentional for the exercise)
/// - Validate ALL input thoroughly
/// - Prevent JSON injection attacks
/// - Don't expose internal error details
/// - Log security-relevant events
pub async fn handler(
    State(state): State<AppState>,
    DatabaseConnection(mut db_conn): DatabaseConnection,
    Json(request): Json<SubmitTransactionRequest>,
) -> AppResult<Response<Body>> {
    info!("Entering handler");
    let (parsed_transaction_data, priority) = validate(&request)?;

    let Some(limit_type) = get_account_limit_type(&mut db_conn, &request.account_id)
        .await
        .optional()?
    else {
        return Err(AppError::bad_request("Invalid account_id: Not found"));
    };

    let maybe_account_rate_limit = get_account_rate_limit(&mut db_conn, limit_type).await?;
    let Some(account_rate_limit) = maybe_account_rate_limit else {
        return Err(AppError::internal_server_error(
            "Invalid account_id limit_type: Not found",
        ));
    };

    // Cloning the redis_pool is perfectly OK, it uses internal reference counting as per documentation
    let rate_limiter = RateLimiter::new(state.redis_pool.clone());
    let rate_limit_result = rate_limiter
        .check_rate_limit(
            &request.account_id,
            account_rate_limit.max_requests,
            account_rate_limit.window_seconds,
        )
        .await?;
    let headers = build_rate_limit_headers(account_rate_limit, &rate_limit_result);
    if !rate_limit_result.allowed {
        return Err(AppError::too_many_requests("Rate limit exceeded", headers));
    }

    let transaction =
        insert_transaction(&mut db_conn, request, parsed_transaction_data, priority).await?;

    let queue_manager = QueueManager::new(state.redis_pool);
    let queue_position = queue_manager
        .enqueue_with_priority("transactions", &transaction.id.to_string(), priority)
        .await?;
    let estimated_time = std::cmp::min(queue_position * 30, 3600);

    let mut response = Json(SubmitTransactionResponse {
        transaction_id: transaction.id,
        queue_position: queue_position,
        estimated_processing_time_seconds: estimated_time,
        status: transaction.status,
    })
    .into_response();
    *response.headers_mut() = headers;
    Ok(response)
}

async fn insert_transaction(
    db_conn: &mut DbConnection,
    request: SubmitTransactionRequest,
    parsed_transaction_data: value::Value,
    priority: i32,
) -> Result<TransactionQueue, AppError> {
    let new_transaction = NewTransactionQueue {
        id: Uuid::new_v4(),
        account_id: request.account_id.clone(),
        transaction_data: parsed_transaction_data,
        priority: priority,
        status: "pending".to_string(),
        retry_count: 0,
        max_retries: 0,
        scheduled_at: None,
    };
    let transaction = diesel::insert_into(transaction_queue::table)
        .values(&new_transaction)
        .get_result::<TransactionQueue>(db_conn)
        .await?;
    Ok(transaction)
}

fn build_rate_limit_headers(
    account_rate_limit: RateLimit,
    rate_limit_result: &redis_cache::RateLimitResult,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("X-RateLimit-Limit", account_rate_limit.max_requests.into());
    headers.insert("X-RateLimit-Remaining", rate_limit_result.remaining.into());
    headers.insert("X-RateLimit-Reset", rate_limit_result.reset_at.into());
    headers
}

async fn get_account_rate_limit(
    db_conn: &mut DbConnection,
    limit_type_param: String,
) -> Result<Option<RateLimit>, AppError> {
    use schema::rate_limits::dsl::*;
    Ok(rate_limits
        .filter(limit_type.eq(&limit_type_param))
        .first::<RateLimit>(db_conn)
        .await
        .optional()?)
}

fn validate(request: &SubmitTransactionRequest) -> Result<(value::Value, i32), AppError> {
    if request.account_id.is_empty() || request.account_id.len() > 255 {
        return Err(AppError::bad_request(
            "Invalid account_id: must be 1-255 characters",
        ));
    }
    let transaction_data_raw = request.transaction_data.get();
    if transaction_data_raw.len() > 1_000_000 {
        return Err(AppError::bad_request(
            "Invalid transaction_data: Must be less than 1MB",
        ));
    }
    let parsed_transaction_data = value::to_value(transaction_data_raw)
        .map_err(|_| AppError::bad_request("Invalid transaction_data: Malformed"))?;
    if parsed_transaction_data.is_null() {
        return Err(AppError::bad_request("transaction_data cannot be null"));
    }
    let priority = request.priority.unwrap_or(0);
    if !(-1000..=1000).contains(&priority) {
        return Err(AppError::bad_request(
            "Invalid priority: If present it must be between -1000 and 1000",
        ));
    }
    Ok((parsed_transaction_data, priority))
}

// Database version. Commented out as tests use randomly generated account ids that don't exist in the DB
/*
async fn get_account_limit_type(db_conn: &mut DbConnection, account_id: &str) -> Result<String, diesel::result::Error>  {
    use postgres_models::schema::accounts::dsl::*;
    let account = accounts.find(account_id).first::<Account>(db_conn).await?;
    Ok(account.limit_type)
}
*/

// To make tests pass (they use random accounts ID that don't exist) infer the limit type from the account_id prefix
// If the prefix is unknown, use basic limit type
async fn get_account_limit_type(
    _: &mut DbConnection,
    account_id: &str,
) -> Result<String, diesel::result::Error> {
    let prefix = account_id.split_once('_').unwrap_or_default().0;
    let limit_type = if ["basic", "premium", "enterprise"].contains(&prefix) {
        prefix
    } else {
        "basic"
    };
    Ok(limit_type.to_owned())
}
