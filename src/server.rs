//! `lt serve`: the HTTP API in docs/specs/http-api.md over a local `Database`.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Body,
    body::Bytes,
    extract::{DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use serde_json::json;

use crate::attachments::{MAX_ATTACHMENT_BYTES, NewAttachment};
use crate::db::{Conflict, Database, Invalid, NotFound, TaskUpdates, now_iso};
use crate::questions::{Answers, Question};

#[derive(Clone)]
struct AppState {
    db: Arc<Database>,
    tokens: Arc<Vec<String>>,
    rules_file: Option<Arc<PathBuf>>,
}

/// The token-protected API, as published through the Cloudflare tunnel.
pub fn router(db: Database, tokens: Vec<String>, rules_file: Option<PathBuf>) -> Router {
    authorized_api(app_state(Arc::new(db), tokens, rules_file))
}

/// The dashboard at `/` plus the API without bearer auth. Bind it only to an
/// interface that the network already restricts (the home WireGuard tunnel).
pub fn ui_router(db: Database, rules_file: Option<PathBuf>) -> Router {
    dashboard(app_state(Arc::new(db), Vec::new(), rules_file))
}

fn app_state(db: Arc<Database>, tokens: Vec<String>, rules_file: Option<PathBuf>) -> AppState {
    AppState {
        db,
        tokens: Arc::new(tokens),
        rules_file: rules_file.map(Arc::new),
    }
}

fn authorized_api(state: AppState) -> Router {
    api(state.clone()).layer(middleware::from_fn_with_state(state, authorize))
}

fn dashboard(state: AppState) -> Router {
    api(state).route("/", get(dashboard_page))
}

async fn dashboard_page() -> Response {
    (
        [(CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../ui/index.html"),
    )
        .into_response()
}

fn api(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/rules", get(rules))
        .route("/events", get(events_after))
        .route("/tasks", post(create_task).get(list_tasks))
        .route("/tasks/claim", post(claim_next))
        .route("/tasks/ready", get(ready_tasks))
        .route("/tasks/due", get(due_tasks))
        .route("/tasks/{id}/delivered", post(mark_delivered))
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
        .route(
            "/tasks/{id}/attachments",
            get(list_attachments)
                .post(add_attachment)
                .layer(DefaultBodyLimit::max(MAX_ATTACHMENT_BYTES)),
        )
        .route("/attachments/{id}", get(get_attachment))
        .route(
            "/tasks/{id}/questions",
            get(list_questions).post(ask_questions),
        )
        .route("/questions/{id}/answer", post(answer_questions))
        .with_state(state)
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

pub async fn serve(
    db: Database,
    addr: SocketAddr,
    token_file: &Path,
    rules_file: Option<PathBuf>,
    ui_addr: Option<SocketAddr>,
) -> anyhow::Result<()> {
    let state = app_state(Arc::new(db), load_tokens(token_file)?, rules_file);
    let api_server = serve_router(addr, authorized_api(state.clone()));
    match ui_addr {
        Some(ui_addr) => {
            tokio::try_join!(api_server, serve_router(ui_addr, dashboard(state)))?;
        }
        None => api_server.await?,
    }
    Ok(())
}

async fn serve_router(addr: SocketAddr, app: Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
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
    } else if error.downcast_ref::<Invalid>().is_some() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, Json(json!({"error": error.to_string()}))).into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

/// Agent rules as markdown, read per request so edits apply without a restart.
async fn rules(State(state): State<AppState>) -> Response {
    let Some(path) = state.rules_file else {
        return error_response(NotFound("rules not configured".to_owned()).into());
    };
    match std::fs::read_to_string(path.as_path()) {
        Ok(text) => ([("content-type", "text/markdown; charset=utf-8")], text).into_response(),
        Err(error) => error_response(anyhow::anyhow!("reading rules file: {error}")),
    }
}

#[derive(Deserialize)]
struct CreateTask {
    /// Optional in the type only, so a missing project is a 400 with a clear message.
    project: Option<String>,
    title: String,
    description: Option<String>,
    priority: Option<u8>,
}

async fn create_task(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateTask>,
) -> Response {
    let Some(project) = body.project else {
        return error_response(Invalid("project is required".to_owned()).into());
    };
    let created = state
        .db
        .create_task(
            &project,
            &body.title,
            body.description.as_deref(),
            body.priority.unwrap_or(0),
            actor(&headers),
        )
        .await;
    result_response(created, StatusCode::CREATED)
}

#[derive(Deserialize)]
struct TaskFilters {
    status: Option<String>,
    assignee: Option<String>,
    project: Option<String>,
}

async fn list_tasks(State(state): State<AppState>, Query(filters): Query<TaskFilters>) -> Response {
    result_response(
        state
            .db
            .list_tasks(
                filters.status.as_deref(),
                filters.assignee.as_deref(),
                filters.project.as_deref(),
            )
            .await,
        StatusCode::OK,
    )
}

async fn ready_tasks(State(state): State<AppState>) -> Response {
    result_response(state.db.ready_tasks().await, StatusCode::OK)
}

async fn due_tasks(State(state): State<AppState>) -> Response {
    result_response(state.db.due_tasks(&now_iso()).await, StatusCode::OK)
}

#[derive(Deserialize)]
struct MarkDelivered {
    due_at: String,
}

async fn mark_delivered(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<MarkDelivered>,
) -> Response {
    no_content(
        state
            .db
            .mark_delivered(&id, &body.due_at, actor(&headers))
            .await,
    )
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
    due_at: Option<String>,
    project: Option<String>,
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
                    due_at: body.due_at.as_deref(),
                    project: body.project.as_deref(),
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
    Json(body): Json<ClaimProject>,
) -> Response {
    let Some(project) = body.project.filter(|value| !value.trim().is_empty()) else {
        return error_response(Invalid("project is required".to_owned()).into());
    };
    no_content(state.db.claim_task(&id, &project, actor(&headers)).await)
}

#[derive(Deserialize)]
struct ClaimProject {
    /// Optional in the type only, so a missing project is a 400 with a clear message.
    project: Option<String>,
}

async fn claim_next(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ClaimProject>,
) -> Response {
    let Some(project) = body.project.filter(|value| !value.trim().is_empty()) else {
        return error_response(Invalid("project is required".to_owned()).into());
    };
    match state.db.claim_next(&project, actor(&headers)).await {
        Ok(Some(task)) => (StatusCode::OK, Json(task)).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error_response(error),
    }
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

#[derive(Deserialize)]
struct EventsAfter {
    after: Option<i64>,
}

async fn events_after(State(state): State<AppState>, Query(query): Query<EventsAfter>) -> Response {
    result_response(
        state.db.events_after(query.after.unwrap_or(0)).await,
        StatusCode::OK,
    )
}

async fn get_events(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Response {
    result_response(state.db.get_events(&id).await, StatusCode::OK)
}

#[derive(Deserialize)]
struct AttachmentName {
    name: String,
}

async fn add_attachment(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<AttachmentName>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mime_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream");
    let file = NewAttachment {
        name: &query.name,
        mime_type,
        data: &body,
    };
    result_response(
        state.db.add_attachment(&id, file, actor(&headers)).await,
        StatusCode::CREATED,
    )
}

async fn list_attachments(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Response {
    result_response(state.db.list_attachments(&id).await, StatusCode::OK)
}

/// JSON with every non-ASCII character escaped, so it is a valid header value.
fn ascii_json(value: &impl serde::Serialize) -> String {
    let json = serde_json::to_string(value).unwrap_or_default();
    json.chars().fold(String::new(), |mut out, c| {
        if c.is_ascii() {
            out.push(c);
        } else {
            for unit in c.encode_utf16(&mut [0; 2]) {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
        out
    })
}

/// RFC 6266: an ASCII `filename` for old clients plus the exact UTF-8 name in `filename*`.
fn content_disposition(name: &str) -> String {
    let ascii: String = name
        .chars()
        .map(|c| match c {
            ' '..='~' if c != '"' && c != '\\' => c,
            _ => '_',
        })
        .collect();
    let encoded: String =
        name.bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' => (byte as char).to_string(),
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|'
                | b'~' => (byte as char).to_string(),
                _ => format!("%{byte:02X}"),
            })
            .collect();
    format!("attachment; filename=\"{ascii}\"; filename*=UTF-8''{encoded}")
}

/// The file bytes, with its metadata in `X-LT-Attachment` (JSON).
async fn get_attachment(State(state): State<AppState>, AxumPath(id): AxumPath<i64>) -> Response {
    let (attachment, data) = match state.db.get_attachment(id).await {
        Ok(found) => found,
        Err(error) => return error_response(error),
    };
    let header = |text: String| {
        HeaderValue::from_str(&text).unwrap_or_else(|_| HeaderValue::from_static(""))
    };
    let disposition = content_disposition(&attachment.name);
    let headers = [
        (CONTENT_TYPE, header(attachment.mime_type.clone())),
        (CONTENT_DISPOSITION, header(disposition)),
    ];
    let mut response = (headers, data).into_response();
    response
        .headers_mut()
        .insert("x-lt-attachment", header(ascii_json(&attachment)));
    response
}

#[derive(Deserialize)]
struct AskQuestions {
    questions: Vec<Question>,
}

async fn ask_questions(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<AskQuestions>,
) -> Response {
    result_response(
        state
            .db
            .ask_questions(&id, body.questions, actor(&headers))
            .await,
        StatusCode::CREATED,
    )
}

async fn list_questions(State(state): State<AppState>, AxumPath(id): AxumPath<String>) -> Response {
    result_response(state.db.list_questions(&id).await, StatusCode::OK)
}

#[derive(Deserialize)]
struct AnswerQuestions {
    answers: Answers,
}

async fn answer_questions(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    headers: HeaderMap,
    Json(body): Json<AnswerQuestions>,
) -> Response {
    no_content(
        state
            .db
            .answer_questions(id, body.answers, actor(&headers))
            .await,
    )
}
