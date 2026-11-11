use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::Request,
    response::Response,
    routing::any,
};
use llm_tasks::{
    db::{Conflict, NotFound, TaskUpdates},
    remote::HttpStore,
    store::TaskStore,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    actor: Option<String>,
    body: Value,
}

#[derive(Clone, Default)]
struct StubState(Arc<Mutex<Vec<RecordedRequest>>>);

// Keep this helper boundary so tests can later use server::router instead.
async fn spawn_test_api() -> (String, StubState) {
    let state = StubState::default();
    let app = Router::new()
        .fallback(any(record_request))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/",), state)
}

async fn record_request(State(state): State<StubState>, request: Request<Body>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    state.0.lock().unwrap().push(RecordedRequest {
        method: parts.method.to_string(),
        path: parts.uri.to_string(),
        authorization: parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        actor: parts
            .headers
            .get("x-lt-actor")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        body: if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    });

    let (status, body) = match parts.uri.path() {
        "/tasks/not-found" => (404, json!({"error": "task absent"})),
        "/tasks/conflict/claim" => (409, json!({"error": "claim denied"})),
        "/tasks/failure" => (500, json!({"error": "database offline"})),
        "/tasks" if parts.method == "POST" => (201, task_json()),
        "/tasks/task%20one" => (200, task_json()),
        "/tasks/task one" => (200, task_json()),
        "/tasks/task one/comments" => (201, comment_json()),
        path if path.ends_with("/comments") && parts.method == "GET" => {
            (200, json!([comment_json()]))
        }
        path if path.ends_with("/dependencies") && parts.method == "GET" => (200, json!([])),
        path if path.ends_with("/events") => (200, json!([])),
        path if path.starts_with("/tasks/") && parts.method == "GET" => (200, task_json()),
        path if path == "/tasks" || path == "/tasks/ready" => (200, json!([task_json()])),
        _ => (204, Value::Null),
    };
    let mut response = Response::new(Body::from(if status == 204 {
        String::new()
    } else {
        body.to_string()
    }));
    *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    response
}

fn task_json() -> Value {
    json!({"id":"task one","title":"Ship it","description":"details","status":"pending","priority":3,"assignee":null,"target_branch":"main","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z"})
}

fn comment_json() -> Value {
    json!({"id":7,"task_id":"task one","actor":"agent-a","content":"looks good","created_at":"2026-01-02T03:04:05Z"})
}

#[tokio::test]
async fn task_and_comment_responses_deserialize_and_requests_follow_contract() {
    let (url, state) = spawn_test_api().await;
    let store = HttpStore::new(&url, "secret").unwrap();

    let created = store
        .create_task("Ship it", Some("details"), 3, "agent-a")
        .await
        .unwrap();
    assert_eq!(created.id, "task one");
    assert_eq!(created.title, "Ship it");
    assert_eq!(created.target_branch.as_deref(), Some("main"));

    let fetched = store.get_task("task one").await.unwrap();
    assert_eq!(fetched.priority, 3);

    let comment = store
        .add_comment("task one", "agent-a", "looks good")
        .await
        .unwrap();
    assert_eq!(comment.id, 7);
    assert_eq!(comment.content, "looks good");

    store
        .update_task(
            "task one",
            TaskUpdates {
                title: Some("Updated"),
                priority: Some(2),
                ..Default::default()
            },
            "agent-a",
        )
        .await
        .unwrap();
    store
        .list_tasks(Some("in progress"), Some("agent-a"))
        .await
        .unwrap();
    store.ready_tasks().await.unwrap();
    store.claim_task("task one", "agent-a").await.unwrap();
    store.close_task("task one", "agent-a").await.unwrap();
    store.get_dependencies("task one").await.unwrap();
    store
        .add_dependency("task one", "blocker/1", "blocks")
        .await
        .unwrap();
    store
        .remove_dependency("task one", "blocker/1")
        .await
        .unwrap();
    store.get_comments("task one").await.unwrap();
    store.get_events("task one").await.unwrap();

    let requests = state.0.lock().unwrap();
    let request = |method: &str, path: &str| {
        requests
            .iter()
            .find(|r| r.method == method && r.path.starts_with(path))
            .unwrap()
    };
    let create = requests
        .iter()
        .find(|r| r.method == "POST" && r.path == "/tasks")
        .unwrap();
    assert_eq!(create.authorization.as_deref(), Some("Bearer secret"));
    assert_eq!(create.actor.as_deref(), Some("agent-a"));
    assert_eq!(
        create.body,
        json!({"title":"Ship it","description":"details","priority":3})
    );
    let fetched = request("GET", "/tasks/task%20one");
    assert_eq!(fetched.authorization.as_deref(), Some("Bearer secret"));
    assert_eq!(fetched.actor, None);
    let comment = request("POST", "/tasks/task%20one/comments");
    assert_eq!(comment.actor.as_deref(), Some("agent-a"));
    assert_eq!(comment.body, json!({"content":"looks good"}));
    assert_eq!(
        request("PATCH", "/tasks/task%20one").body,
        json!({"title":"Updated","priority":2})
    );
    assert!(
        request("GET", "/tasks?status=in+progress&assignee=agent-a")
            .authorization
            .is_some()
    );
    assert!(request("GET", "/tasks/ready").authorization.is_some());
    assert_eq!(
        request("POST", "/tasks/task%20one/claim").actor.as_deref(),
        Some("agent-a")
    );
    assert_eq!(
        request("POST", "/tasks/task%20one/close").actor.as_deref(),
        Some("agent-a")
    );
    assert_eq!(
        request("POST", "/tasks/task%20one/dependencies").body,
        json!({"depends_on":"blocker/1","dep_type":"blocks"})
    );
    assert!(
        request("DELETE", "/tasks/task%20one/dependencies/blocker%2F1")
            .authorization
            .is_some()
    );
}

#[tokio::test]
async fn api_errors_preserve_typed_messages_and_surface_other_statuses() {
    let (url, _) = spawn_test_api().await;
    let store = HttpStore::new(&url, "secret").unwrap();

    let error = store.get_task("not-found").await.unwrap_err();
    assert_eq!(error.downcast_ref::<NotFound>().unwrap().0, "task absent");
    let error = store.claim_task("conflict", "agent-a").await.unwrap_err();
    assert_eq!(error.downcast_ref::<Conflict>().unwrap().0, "claim denied");
    let error = store.get_task("failure").await.unwrap_err().to_string();
    assert!(error.contains("500"));
    assert!(error.contains("database offline"));
}
