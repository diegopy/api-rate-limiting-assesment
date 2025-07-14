use transaction_queue_api::AppState;
use axum::{
    async_trait,
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
};
use postgres_models::DbConnection;

pub struct DatabaseConnection(pub DbConnection);

#[async_trait]
impl<S> FromRequestParts<S> for DatabaseConnection
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        let conn = app_state
            .db_pool
            .get_owned()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        
        Ok(DatabaseConnection(conn))
    }
}

pub struct ReadOnlyDatabaseConnection(pub DbConnection);

#[async_trait]
impl<S> FromRequestParts<S> for ReadOnlyDatabaseConnection
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        let conn = app_state
            .db_pool
            .get_owned()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        
        Ok(ReadOnlyDatabaseConnection(conn))
    }
}