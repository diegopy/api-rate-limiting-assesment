use axum::Router;

mod transactions;

pub fn router() -> Router<transaction_queue_api::AppState> {
    Router::new()
        .nest("/transactions", transactions::router())
}