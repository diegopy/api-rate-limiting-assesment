use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::{Router, Json};
use dotenvy::dotenv;
use serde_json::json;
use tower_http::limit::RequestBodyLimitLayer;
use std::net::SocketAddr;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::{info, Level};

pub mod config;
mod errors;
mod extractors;
mod v1;

use crate::config::Config;
use transaction_queue_api::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "transaction_queue_api=debug,tower_http=debug".into()),
        )
        .init();

    // Load configuration
    let config = Config::from_env()?;

    // Create application state
    let state = AppState::new(&config.database_url, &config.redis_url).await?;

    // Health check endpoint
    async fn health() -> Json<serde_json::Value> {
        Json(json!({
            "status": "ok",
            "service": "transaction-queue-api"
        }))
    }

    // Build the application
    let app = Router::new()
        .route("/health", axum::routing::get(health))
        .nest("/v1", v1::router())
        .layer(
            ServiceBuilder::new()
                .layer(RequestBodyLimitLayer::new(2 * 1024 * 1024))
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO)),
                )
                .layer(CorsLayer::permissive()),
        )
        .with_state(state);

    // Start the server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!("Starting server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}