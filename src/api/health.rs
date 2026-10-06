//! Health probes — the only listener this service opens (ADR-0002 as
//! amended by ADR-0004): liveness says the event loop is up; readiness says
//! the database answers and shutdown has not begun.

use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

#[derive(Clone)]
pub(crate) struct HealthState {
    pool: PgPool,
    shutting_down: Arc<AtomicBool>,
}

impl HealthState {
    pub(crate) fn new(pool: PgPool, shutting_down: Arc<AtomicBool>) -> Self {
        Self {
            pool,
            shutting_down,
        }
    }
}

pub(crate) fn router(state: HealthState) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(state)
}

async fn livez() -> &'static str {
    "live"
}

async fn readyz(State(state): State<HealthState>) -> StatusCode {
    if state.shutting_down.load(Ordering::Relaxed) {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let check = tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query("SELECT 1").execute(&state.pool),
    )
    .await;
    match check {
        Ok(Ok(_)) => StatusCode::OK,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn state_with(shutting_down: bool) -> (HealthState, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(shutting_down));
        // Readiness's DB check fails against this lazy pool (no connection)
        // — tests assert the live/shutdown branches, and the DB branch is
        // exercised by deployment probes.
        let pool = sqlx::Pool::<sqlx::Postgres>::connect_lazy("postgres://127.0.0.1:1/none")
            .expect("lazy pool");
        (HealthState::new(pool, flag.clone()), flag)
    }

    #[tokio::test]
    async fn livez_answers_live() {
        let (state, _flag) = state_with(false);
        let app = router(state);

        let response = app
            .oneshot(Request::get("/livez").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 16)
            .await
            .unwrap();
        assert_eq!(&body[..], b"live");
    }

    #[tokio::test]
    async fn readyz_flips_unavailable_on_shutdown() {
        let (state, _flag) = state_with(true);
        let app = router(state);

        let response = app
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_answers_unavailable_when_database_does_not_answer() {
        let (state, _flag) = state_with(false);
        let app = router(state);

        let response = app
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        // The unreachable pool fails the DB check within the timeout.
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
