use axum::{routing::post, Router};

mod submit;

pub fn router() -> Router<transaction_queue_api::AppState> {
    Router::new()
        .route("/submit", post(submit::handler))
}