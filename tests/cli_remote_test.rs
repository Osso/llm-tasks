//! End to end: a real `lt serve` process and `lt` clients in LT_URL mode.

use std::process::{Child, Command, Output};
use std::time::Duration;

use tempfile::TempDir;

const LT: &str = env!("CARGO_BIN_EXE_lt");
const TOKEN: &str = "test-token-1";

struct Server {
    child: Child,
    url: String,
    _dir: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn start_server() -> Server {
    let dir = TempDir::new().unwrap();
    let tokens = dir.path().join("tokens");
    std::fs::write(&tokens, format!("other-token\n\n{TOKEN}\n")).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(LT)
        .arg("--db")
        .arg(dir.path().join("tasks.db"))
        .args([
            "serve",
            "--addr",
            &format!("127.0.0.1:{port}"),
            "--token-file",
        ])
        .arg(&tokens)
        .spawn()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if reqwest::get(format!("{url}/healthz")).await.is_ok() {
            return Server {
                child,
                url,
                _dir: dir,
            };
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("lt serve did not become healthy");
}

fn lt(server: &Server, token: &str, actor: &str, args: &[&str]) -> Output {
    Command::new(LT)
        .env("LT_URL", &server.url)
        .env("LT_TOKEN", token)
        .env("LT_ACTOR", actor)
        .env_remove("LT_DB")
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    assert!(
        output.status.success(),
        "lt failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).unwrap()
}

#[tokio::test]
async fn remote_cli_round_trip_and_single_claim_winner() {
    let server = start_server().await;

    let created = stdout(&lt(
        &server,
        TOKEN,
        "planner",
        &["--json", "create", "Ship tracker", "-p", "2"],
    ));
    let task: serde_json::Value = serde_json::from_str(&created).unwrap();
    let id = task["id"].as_str().unwrap().to_owned();
    assert_eq!(task["title"], "Ship tracker");
    assert_eq!(task["priority"], 2);

    assert!(stdout(&lt(&server, TOKEN, "worker-a", &["ready"])).contains(&id));

    let first = lt(&server, TOKEN, "worker-a", &["claim", &id]);
    assert!(stdout(&first).contains("Claimed"));
    let second = lt(&server, TOKEN, "worker-b", &["claim", &id]);
    assert!(!second.status.success());
    assert!(
        String::from_utf8_lossy(&second.stderr).contains("already assigned to worker-a"),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(!stdout(&lt(&server, TOKEN, "worker-a", &["ready"])).contains(&id));

    stdout(&lt(&server, TOKEN, "worker-a", &["close", &id]));
    let shown: serde_json::Value = serde_json::from_str(&stdout(&lt(
        &server,
        TOKEN,
        "worker-a",
        &["--json", "show", &id],
    )))
    .unwrap();
    assert_eq!(shown["status"], "completed");
    assert_eq!(shown["assignee"], "worker-a");

    let history = stdout(&lt(&server, TOKEN, "worker-a", &["history", &id]));
    assert!(history.contains("planner created"));
    assert!(history.contains("worker-a claimed"));
    assert!(history.contains("worker-a closed"));
}

#[tokio::test]
async fn remote_cli_rejects_unknown_token_and_missing_task() {
    let server = start_server().await;

    let unauthorized = lt(&server, "wrong-token", "intruder", &["list"]);
    assert!(!unauthorized.status.success());
    assert!(String::from_utf8_lossy(&unauthorized.stderr).contains("401"));

    let missing = lt(&server, TOKEN, "worker-a", &["show", "lt-none"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("Task not found: lt-none"));
}
