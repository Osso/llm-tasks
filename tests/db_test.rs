use llm_tasks::db::Database;
use tempfile::TempDir;

async fn temp_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = Database::open(&path).await.unwrap();
    (db, dir)
}

#[tokio::test]
async fn create_task_returns_pending() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Build feature", Some("Details"), 2, "agent-1").await.unwrap();

    assert!(task.id.starts_with("lt-"));
    assert_eq!(task.title, "Build feature");
    assert_eq!(task.description.as_deref(), Some("Details"));
    assert_eq!(task.status, "pending");
    assert_eq!(task.priority, 2);
    assert!(task.assignee.is_none());
}

#[tokio::test]
async fn create_task_without_description() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("No desc", None, 0, "agent-1").await.unwrap();

    assert_eq!(task.title, "No desc");
    assert!(task.description.is_none() || task.description.as_deref() == Some(""));
}

#[tokio::test]
async fn get_task_not_found() {
    let (db, _dir) = temp_db().await;
    let result = db.get_task("lt-9999").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn list_tasks_ordered_by_priority() {
    let (db, _dir) = temp_db().await;
    db.create_task("Low", None, 1, "a").await.unwrap();
    db.create_task("High", None, 3, "a").await.unwrap();
    db.create_task("Med", None, 2, "a").await.unwrap();

    let tasks = db.list_tasks(None, None).await.unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].priority, 3);
    assert_eq!(tasks[1].priority, 2);
    assert_eq!(tasks[2].priority, 1);
}

#[tokio::test]
async fn list_tasks_filter_by_status() {
    let (db, _dir) = temp_db().await;
    let t1 = db.create_task("Done", None, 0, "a").await.unwrap();
    db.create_task("Pending", None, 0, "a").await.unwrap();
    db.close_task(&t1.id, "a").await.unwrap();

    let completed = db.list_tasks(Some("completed"), None).await.unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, t1.id);
}

#[tokio::test]
async fn list_tasks_filter_by_assignee() {
    let (db, _dir) = temp_db().await;
    let t1 = db.create_task("Claimed", None, 0, "a").await.unwrap();
    db.create_task("Unclaimed", None, 0, "a").await.unwrap();
    db.claim_task(&t1.id, "dev-0").await.unwrap();

    let assigned = db.list_tasks(None, Some("dev-0")).await.unwrap();
    assert_eq!(assigned.len(), 1);
    assert_eq!(assigned[0].id, t1.id);
}

#[tokio::test]
async fn claim_sets_assignee_and_in_progress() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Claimable", None, 0, "a").await.unwrap();

    db.claim_task(&task.id, "dev-0").await.unwrap();

    let updated = db.get_task(&task.id).await.unwrap();
    assert_eq!(updated.assignee.as_deref(), Some("dev-0"));
    assert_eq!(updated.status, "in_progress");
}

#[tokio::test]
async fn claim_rejects_already_claimed() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Race", None, 0, "a").await.unwrap();

    db.claim_task(&task.id, "dev-0").await.unwrap();
    let result = db.claim_task(&task.id, "dev-1").await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Cannot claim"));
}

#[tokio::test]
async fn claim_rejects_completed() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Done", None, 0, "a").await.unwrap();
    db.close_task(&task.id, "a").await.unwrap();

    let result = db.claim_task(&task.id, "dev-0").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn close_sets_completed() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Closeable", None, 0, "a").await.unwrap();

    db.close_task(&task.id, "a").await.unwrap();

    let closed = db.get_task(&task.id).await.unwrap();
    assert_eq!(closed.status, "completed");
}

#[tokio::test]
async fn update_changes_fields() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Original", None, 1, "a").await.unwrap();

    db.update_task(&task.id, None, Some(3), Some("Renamed"), Some("New desc"), "a").await.unwrap();

    let updated = db.get_task(&task.id).await.unwrap();
    assert_eq!(updated.title, "Renamed");
    assert_eq!(updated.description.as_deref(), Some("New desc"));
    assert_eq!(updated.priority, 3);
}

#[tokio::test]
async fn update_skips_unchanged_fields() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Same", None, 1, "a").await.unwrap();

    db.update_task(&task.id, Some("pending"), Some(1), Some("Same"), None, "a").await.unwrap();

    let events = db.get_events(&task.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "created");
}

#[tokio::test]
async fn ready_returns_unblocked_only() {
    let (db, _dir) = temp_db().await;
    let blocker = db.create_task("Blocker", None, 3, "a").await.unwrap();
    let blocked = db.create_task("Blocked", None, 2, "a").await.unwrap();
    let free = db.create_task("Free", None, 1, "a").await.unwrap();

    db.add_dependency(&blocked.id, &blocker.id, "blocks").await.unwrap();

    let ready = db.ready_tasks().await.unwrap();
    let ids: Vec<&str> = ready.iter().map(|t| t.id.as_str()).collect();

    assert!(ids.contains(&blocker.id.as_str()));
    assert!(ids.contains(&free.id.as_str()));
    assert!(!ids.contains(&blocked.id.as_str()));
}

#[tokio::test]
async fn completing_blocker_unblocks_dependent() {
    let (db, _dir) = temp_db().await;
    let blocker = db.create_task("Blocker", None, 0, "a").await.unwrap();
    let blocked = db.create_task("Blocked", None, 0, "a").await.unwrap();

    db.add_dependency(&blocked.id, &blocker.id, "blocks").await.unwrap();
    assert!(!db.ready_tasks().await.unwrap().iter().any(|t| t.id == blocked.id));

    db.close_task(&blocker.id, "a").await.unwrap();
    assert!(db.ready_tasks().await.unwrap().iter().any(|t| t.id == blocked.id));
}

#[tokio::test]
async fn chain_dependency_unblock() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("A", None, 0, "x").await.unwrap();
    let b = db.create_task("B", None, 0, "x").await.unwrap();
    let c = db.create_task("C", None, 0, "x").await.unwrap();

    db.add_dependency(&b.id, &a.id, "blocks").await.unwrap();
    db.add_dependency(&c.id, &b.id, "blocks").await.unwrap();

    // Only A is ready
    let ready = db.ready_tasks().await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, a.id);

    // Complete A -> B ready, C still blocked
    db.close_task(&a.id, "x").await.unwrap();
    let ready = db.ready_tasks().await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, b.id);

    // Complete B -> C ready
    db.close_task(&b.id, "x").await.unwrap();
    let ready = db.ready_tasks().await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, c.id);
}

#[tokio::test]
async fn add_dependency_validates_tasks_exist() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Real", None, 0, "a").await.unwrap();

    assert!(db.add_dependency(&task.id, "lt-fake", "blocks").await.is_err());
    assert!(db.add_dependency("lt-fake", &task.id, "blocks").await.is_err());
}

#[tokio::test]
async fn remove_dependency_unblocks() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("A", None, 0, "x").await.unwrap();
    let b = db.create_task("B", None, 0, "x").await.unwrap();

    db.add_dependency(&b.id, &a.id, "blocks").await.unwrap();
    assert!(!db.ready_tasks().await.unwrap().iter().any(|t| t.id == b.id));

    db.remove_dependency(&b.id, &a.id).await.unwrap();
    assert!(db.ready_tasks().await.unwrap().iter().any(|t| t.id == b.id));
}

#[tokio::test]
async fn get_dependencies_returns_deps() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("A", None, 0, "x").await.unwrap();
    let b = db.create_task("B", None, 0, "x").await.unwrap();

    db.add_dependency(&b.id, &a.id, "blocks").await.unwrap();

    let deps = db.get_dependencies(&b.id).await.unwrap();
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].depends_on, a.id);
    assert_eq!(deps[0].dep_type, "blocks");
}

#[tokio::test]
async fn events_track_full_lifecycle() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Tracked", None, 1, "creator").await.unwrap();

    db.claim_task(&task.id, "worker").await.unwrap();
    db.close_task(&task.id, "worker").await.unwrap();

    let events = db.get_events(&task.id).await.unwrap();
    let actions: Vec<&str> = events.iter().map(|e| e.action.as_str()).collect();

    assert_eq!(actions, ["created", "claimed", "updated", "closed"]);
    assert_eq!(events[0].actor, "creator");
    assert_eq!(events[1].actor, "worker");
}

#[tokio::test]
async fn events_track_field_updates() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("Before", None, 1, "a").await.unwrap();

    db.update_task(&task.id, None, Some(3), Some("After"), None, "editor").await.unwrap();

    let events = db.get_events(&task.id).await.unwrap();
    assert_eq!(events.len(), 3); // created + priority + title

    let priority_ev = events.iter().find(|e| e.field.as_deref() == Some("priority")).unwrap();
    assert_eq!(priority_ev.old_value.as_deref(), Some("1"));
    assert_eq!(priority_ev.new_value.as_deref(), Some("3"));

    let title_ev = events.iter().find(|e| e.field.as_deref() == Some("title")).unwrap();
    assert_eq!(title_ev.old_value.as_deref(), Some("Before"));
    assert_eq!(title_ev.new_value.as_deref(), Some("After"));
}
