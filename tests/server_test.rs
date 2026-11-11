use std::{net::SocketAddr, path::Path};

use llm_tasks::{db::Database, server};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

const TOKEN: &str = "test-secret";

async fn start_server() -> (String, reqwest::Client, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::open(&dir.path().join("tasks.db")).await.unwrap();
    let app = server::router(db, vec![TOKEN.to_owned()]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), reqwest::Client::new(), dir)
}

fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
) -> reqwest::RequestBuilder {
    client.request(method, url).bearer_auth(TOKEN)
}

async fn create(client: &reqwest::Client, base: &str, title: &str) -> Value {
    request(client, reqwest::Method::POST, format!("{base}/tasks"))
        .json(&json!({"title": title, "description": "concrete details", "priority": 3}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn auth_is_required_except_healthz() {
    let (base, client, _dir) = start_server().await;
    let no_token = client.get(format!("{base}/tasks")).send().await.unwrap();
    assert_eq!(no_token.status(), reqwest::StatusCode::UNAUTHORIZED);
    let wrong_token = client
        .get(format!("{base}/tasks"))
        .bearer_auth("wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_token.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        wrong_token.json::<Value>().await.unwrap()["error"],
        "Unauthorized"
    );
    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    assert_eq!(health.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn create_get_list_ready_claim_close_records_actor() {
    let (base, client, _dir) = start_server().await;
    let created = create(&client, &base, "HTTP task").await;
    assert_eq!(created["title"], "HTTP task");
    assert_eq!(created["description"], "concrete details");
    let id = created["id"].as_str().unwrap();

    let fetched: Value = request(&client, reqwest::Method::GET, format!("{base}/tasks/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["id"], id);
    let listed: Vec<Value> = request(&client, reqwest::Method::GET, format!("{base}/tasks"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed.iter().any(|task| task["id"] == id));
    let ready: Vec<Value> = request(&client, reqwest::Method::GET, format!("{base}/tasks/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(ready.iter().any(|task| task["id"] == id));

    let claim = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{id}/claim"),
    )
    .header("X-LT-Actor", "worker-7")
    .send()
    .await
    .unwrap();
    assert_eq!(claim.status(), reqwest::StatusCode::NO_CONTENT);
    let close = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{id}/close"),
    )
    .header("X-LT-Actor", "worker-7")
    .send()
    .await
    .unwrap();
    assert_eq!(close.status(), reqwest::StatusCode::NO_CONTENT);
    let final_task: Value = request(&client, reqwest::Method::GET, format!("{base}/tasks/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(final_task["status"], "completed");
    assert_eq!(final_task["assignee"], "worker-7");
    let events: Vec<Value> = request(
        &client,
        reqwest::Method::GET,
        format!("{base}/tasks/{id}/events"),
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["actor"] == "worker-7" && event["action"] == "claimed")
    );
    assert!(
        events
            .iter()
            .any(|event| event["actor"] == "worker-7" && event["action"] == "closed")
    );
    assert!(
        events
            .iter()
            .any(|event| event["actor"] == "unknown" && event["action"] == "created")
    );
}

#[tokio::test]
async fn unknown_task_returns_json_404() {
    let (base, client, _dir) = start_server().await;
    let response = request(
        &client,
        reqwest::Method::GET,
        format!("{base}/tasks/no-such-task"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    assert!(
        response.json::<Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("not found")
    );
}

#[tokio::test]
async fn second_claim_returns_409_and_dependency_changes_readiness() {
    let (base, client, _dir) = start_server().await;
    let blocker = create(&client, &base, "Blocker").await;
    let dependent = create(&client, &base, "Dependent").await;
    let blocker_id = blocker["id"].as_str().unwrap();
    let dependent_id = dependent["id"].as_str().unwrap();

    let added = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{dependent_id}/dependencies"),
    )
    .json(&json!({"depends_on": blocker_id}))
    .send()
    .await
    .unwrap();
    assert_eq!(added.status(), reqwest::StatusCode::NO_CONTENT);
    let dependencies: Vec<Value> = request(
        &client,
        reqwest::Method::GET,
        format!("{base}/tasks/{dependent_id}/dependencies"),
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(dependencies[0]["dep_type"], "blocks");
    let ready: Vec<Value> = request(&client, reqwest::Method::GET, format!("{base}/tasks/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!ready.iter().any(|task| task["id"] == dependent_id));

    let first_claim = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{blocker_id}/claim"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(first_claim.status(), reqwest::StatusCode::NO_CONTENT);
    let second_claim = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{blocker_id}/claim"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(second_claim.status(), reqwest::StatusCode::CONFLICT);
    let close = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/tasks/{blocker_id}/close"),
    )
    .send()
    .await
    .unwrap();
    assert_eq!(close.status(), reqwest::StatusCode::NO_CONTENT);
    let ready: Vec<Value> = request(&client, reqwest::Method::GET, format!("{base}/tasks/ready"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(ready.iter().any(|task| task["id"] == dependent_id));
}

#[tokio::test]
async fn concurrent_http_claims_have_one_winner() {
    let (base, client, _dir) = start_server().await;
    let task = create(&client, &base, "Concurrent claim").await;
    let id = task["id"].as_str().unwrap().to_owned();
    let mut claims = Vec::new();
    for index in 0..16 {
        let client = client.clone();
        let url = format!("{base}/tasks/{id}/claim");
        claims.push(tokio::spawn(async move {
            client
                .post(url)
                .bearer_auth(TOKEN)
                .header("X-LT-Actor", format!("actor-{index}"))
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    let mut successes = 0;
    let mut conflicts = 0;
    for claim in claims {
        match claim.await.unwrap() {
            reqwest::StatusCode::NO_CONTENT => successes += 1,
            reqwest::StatusCode::CONFLICT => conflicts += 1,
            status => panic!("unexpected claim response: {status}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 15);
}

#[tokio::test]
async fn token_file_requires_nonblank_tokens() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("missing");
    assert!(server::load_tokens(Path::new(&missing)).is_err());
    let empty = dir.path().join("empty");
    std::fs::write(&empty, " \n\t\n").unwrap();
    assert!(server::load_tokens(&empty).is_err());
    let valid = dir.path().join("valid");
    std::fs::write(&valid, "  first-token  \n\nsecond-token\n").unwrap();
    assert_eq!(
        server::load_tokens(&valid).unwrap(),
        ["first-token", "second-token"]
    );
}
