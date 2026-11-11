//! `lt serve`: the HTTP API in docs/specs/http-api.md over a local `Database`.

use std::{net::SocketAddr, path::Path, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, Request, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::json;

use crate::db::{Conflict, Database, NotFound, TaskUpdates};

#[derive(Clone)]
struct AppState {
    db: Arc<Database>,
    tokens: Arc<Vec<String>>,
}

pub fn router(db: Database, tokens: Vec<String>) -> Router {
    let state = AppState {
        db: Arc::new(db),
        tokens: Arc::new(tokens),
    };
    Router::new()
        .route("/healthz", get(healthz))
        .route("/tasks", post(create_task).get(list_tasks))
        .route("/tasks/ready", get(ready_tasks))
        .route("/tasks/{id}", get(get_task).patch(update_task))
        .route("/tasks/{id}/claim", post(claim_task))
        .route("/tasks/{id}/close", post(close_task))
        .route(
            "/tasks/{id}/dependencies",
            get(get_dependencies).post(add_dependency),
        )
        .route(
            "/tasks/{id}/dependencies/{depends_on}",
            delete(remove_dependency),
        )
        .route("/tasks/{id}/comments", get(get_comments).post(add_comment))
        .route("/tasks/{id}/events", get(get_events))
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, authorize))
}

pub fn load_tokens(path: &Path) -> anyhow::Result<Vec<String>> {
    let contents = std::fs::read_to_string(path)?;
    let tokens: Vec<String> = contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    anyhow::ensure!(!tokens.is_empty(), "token file contains no tokens");
    Ok(tokens)
}

pub async fn serve(db: Database, addr: SocketAddr, token_file: &Path) -> anyhow::Result<()> {
    let tokens = load_tokens(token_file)?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(db, tokens))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn authorize(State(state): State<AppState>, request: Request<Body>, next: Next) -> Response {
    if request.method() == axum::http::Method::GET && request.uri().path() == "/healthz" {
        return next.run(request).await;
    }
    let authorized = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|candidate| {
            state
                .tokens
                .iter()
                .any(|token| constant_time_eq(candidate.as_bytes(), token.as_bytes()))
        });
    if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Unauthorized"})),
        )
            .into_response()
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (a, b)| difference | (a ^ b))
            == 0
}

fn actor(headers: &HeaderMap) -> &str {
    headers
        .get("X-LT-Actor")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
}

fn result_response<T: serde::Serialize>(result: anyhow::Result<T>, status: StatusCode) -> Response {
    match result {
        Ok(value) => (status, Json(value)).into_response(),
        Err(error) => error_response(error),
    }
}

fn no_content(result: anyhow::Result<()>) -> Response {
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(error),
    }
}

fn error_response(error: anyhow::Error) -> Response {
    let status = if error.downcast_ref::<NotFound>().is_some() {
        StatusCode::NOT_FOUND
    } else if error.downcast_ref::<Conflict>().is_some() {
        StatusCode::CONFLICT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, Json(json!({"error": error.to_string()}))).into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
struct CreateTask {
    title: String,
    description: Option<String>,
    priority: Option<u8>,
}

async fn create_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateTask>,
) -> Response {
    result_response(
        state
            .db
            .create_task(
                &body.title,
                body.description.as_deref(),
                body.priority.unwrap_or(0),
                actor(&headers),
            )
            .await,
        StatusCode::CREATED,
    )
}

#[derive(Deserialize)]
struct TaskFilters {
    status: Option<String>,
    assignee: Option<String>,
}

async fn list_tasks(State(state): State<AppState>, Query(filters): Query<TaskFilters>) -> Response {
    result_response(
        state
            .db
            .list_tasks(filters.status.as_deref(), filters.assignee.as_deref())
            .await,
        StatusCode::OK,
    )
}

async fn ready_tasks(State(state): State<AppState>) -> Response {
    result_response(state.db.ready_tasks().await, StatusCode::OK)
}

async fn get_task(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Response {
    result_response(state.db.get_task(&id).await, StatusCode::OK)
}

#[derive(Deserialize)]
struct UpdateBody {
    status: Option<String>,
    priority: Option<u8>,
    title: Option<String>,
    description: Option<String>,
    assignee: Option<String>,
    target_branch: Option<String>,
}

async fn update_task(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<UpdateBody>,
) -> Response {
    no_content(
        state
            .db
            .update_task(
                &id,
                TaskUpdates {
                    status: body.status.as_deref(),
                    priority: body.priority,
                    title: body.title.as_deref(),
                    description: body.description.as_deref(),
                    assignee: body.assignee.as_deref(),
                    target_branch: body.target_branch.as_deref(),
                },
                actor(&headers),
            )
            .await,
    )
}

async fn claim_task(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    no_content(state.db.claim_task(&id, actor(&headers)).await)
}

async fn close_task(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    no_content(state.db.close_task(&id, actor(&headers)).await)
}

async fn get_dependencies(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    result_response(state.db.get_dependencies(&id).await, StatusCode::OK)
}

#[derive(Deserialize)]
struct AddDependency {
    depends_on: String,
    dep_type: Option<String>,
}

async fn add_dependency(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<AddDependency>,
) -> Response {
    no_content(
        state
            .db
            .add_dependency(
                &id,
                &body.depends_on,
                body.dep_type.as_deref().unwrap_or("blocks"),
            )
            .await,
    )
}

async fn remove_dependency(
    State(state): State<AppState>,
    AxumPath((id, depends_on)): AxumPath<(String, String)>,
) -> Response {
    no_content(state.db.remove_dependency(&id, &depends_on).await)
}

#[derive(Deserialize)]
struct AddComment {
    content: String,
}

async fn get_comments(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Response {
    result_response(state.db.get_comments(&id).await, StatusCode::OK)
}

async fn add_comment(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<AddComment>,
) -> Response {
    result_response(
        state
            .db
            .add_comment(&id, actor(&headers), &body.content)
            .await,
        StatusCode::CREATED,
    )
}

async fn get_events(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Response {
    result_response(state.db.get_events(&id).await, StatusCode::OK)
}
