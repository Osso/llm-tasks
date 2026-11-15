use llm_tasks::db::{Conflict, Database, NotFound, TaskUpdates};
use tempfile::TempDir;

async fn temp_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let db = Database::open(&path).await.unwrap();
    (db, dir)
}

#[tokio::test]
async fn open_creates_parent_directories() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nested").join("state").join("tasks.db");

    let _db = Database::open(&path).await.unwrap();

    assert!(path.exists());
}

#[tokio::test]
async fn create_task_returns_pending() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Build feature", Some("Details"), 2, "agent-1")
        .await
        .unwrap();

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
    let task = db
        .create_task("test", "No desc", None, 0, "agent-1")
        .await
        .unwrap();

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
    db.create_task("test", "Low", None, 1, "a").await.unwrap();
    db.create_task("test", "High", None, 3, "a").await.unwrap();
    db.create_task("test", "Med", None, 2, "a").await.unwrap();

    let tasks = db.list_tasks(None, None, None).await.unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].priority, 3);
    assert_eq!(tasks[1].priority, 2);
    assert_eq!(tasks[2].priority, 1);
}

#[tokio::test]
async fn list_tasks_filter_by_status() {
    let (db, _dir) = temp_db().await;
    let t1 = db.create_task("test", "Done", None, 0, "a").await.unwrap();
    db.create_task("test", "Pending", None, 0, "a")
        .await
        .unwrap();
    db.close_task(&t1.id, "a").await.unwrap();

    let completed = db.list_tasks(Some("completed"), None, None).await.unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, t1.id);
}

#[tokio::test]
async fn list_tasks_filter_by_assignee() {
    let (db, _dir) = temp_db().await;
    let t1 = db
        .create_task("test", "Claimed", None, 0, "a")
        .await
        .unwrap();
    db.create_task("test", "Unclaimed", None, 0, "a")
        .await
        .unwrap();
    db.claim_task(&t1.id, "dev-0").await.unwrap();

    let assigned = db.list_tasks(None, Some("dev-0"), None).await.unwrap();
    assert_eq!(assigned.len(), 1);
    assert_eq!(assigned[0].id, t1.id);
}

#[tokio::test]
async fn list_tasks_filters_by_status_and_assignee() {
    let (db, _dir) = temp_db().await;
    let matching = db
        .create_task("test", "Matching", None, 0, "a")
        .await
        .unwrap();
    let wrong_assignee = db
        .create_task("test", "Wrong assignee", None, 0, "a")
        .await
        .unwrap();
    let wrong_status = db
        .create_task("test", "Wrong status", None, 0, "a")
        .await
        .unwrap();

    db.claim_task(&matching.id, "dev-0").await.unwrap();
    db.claim_task(&wrong_assignee.id, "dev-1").await.unwrap();
    db.claim_task(&wrong_status.id, "dev-0").await.unwrap();
    db.close_task(&wrong_status.id, "dev-0").await.unwrap();

    let tasks = db
        .list_tasks(Some("in_progress"), Some("dev-0"), None)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, matching.id);
}

#[tokio::test]
async fn claim_sets_assignee_and_in_progress() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Claimable", None, 0, "a")
        .await
        .unwrap();

    db.claim_task(&task.id, "dev-0").await.unwrap();

    let updated = db.get_task(&task.id).await.unwrap();
    assert_eq!(updated.assignee.as_deref(), Some("dev-0"));
    assert_eq!(updated.status, "in_progress");
}

#[tokio::test]
async fn claim_rejects_already_claimed() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("test", "Race", None, 0, "a").await.unwrap();

    db.claim_task(&task.id, "dev-0").await.unwrap();
    let result = db.claim_task(&task.id, "dev-1").await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Cannot claim"));
}

#[tokio::test]
async fn claim_rejects_ready_task_with_assignee() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("test", "Race", None, 0, "a").await.unwrap();
    db.claim_task(&task.id, "dev-0").await.unwrap();
    db.update_task(
        &task.id,
        TaskUpdates {
            status: Some("ready"),
            ..Default::default()
        },
        "runtime",
    )
    .await
    .unwrap();

    let err = db
        .claim_task(&task.id, "dev-1")
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("already assigned to dev-0"));
}

#[tokio::test]
async fn claim_rejects_completed() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("test", "Done", None, 0, "a").await.unwrap();
    db.close_task(&task.id, "a").await.unwrap();

    let result = db.claim_task(&task.id, "dev-0").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn close_sets_completed() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Closeable", None, 0, "a")
        .await
        .unwrap();

    db.close_task(&task.id, "a").await.unwrap();

    let closed = db.get_task(&task.id).await.unwrap();
    assert_eq!(closed.status, "completed");
}

#[tokio::test]
async fn update_changes_fields() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Original", None, 1, "a")
        .await
        .unwrap();

    db.update_task(
        &task.id,
        TaskUpdates {
            priority: Some(3),
            title: Some("Renamed"),
            description: Some("New desc"),
            ..Default::default()
        },
        "a",
    )
    .await
    .unwrap();

    let updated = db.get_task(&task.id).await.unwrap();
    assert_eq!(updated.title, "Renamed");
    assert_eq!(updated.description.as_deref(), Some("New desc"));
    assert_eq!(updated.priority, 3);
}

#[tokio::test]
async fn update_empty_optional_fields_clears_to_null() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task_with_branch("test", "Original", Some("desc"), 1, "a", Some("feature"))
        .await
        .unwrap();

    db.update_task(
        &task.id,
        TaskUpdates {
            description: Some(""),
            assignee: Some(""),
            target_branch: Some(""),
            ..Default::default()
        },
        "a",
    )
    .await
    .unwrap();

    let updated = db.get_task(&task.id).await.unwrap();
    assert!(updated.description.is_none());
    assert!(updated.assignee.is_none());
    assert!(updated.target_branch.is_none());
}

#[tokio::test]
async fn update_skips_unchanged_fields() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("test", "Same", None, 1, "a").await.unwrap();

    db.update_task(
        &task.id,
        TaskUpdates {
            status: Some("pending"),
            priority: Some(1),
            title: Some("Same"),
            ..Default::default()
        },
        "a",
    )
    .await
    .unwrap();

    let events = db.get_events(&task.id).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "created");
}

#[tokio::test]
async fn ready_returns_unblocked_only() {
    let (db, _dir) = temp_db().await;
    let blocker = db
        .create_task("test", "Blocker", None, 3, "a")
        .await
        .unwrap();
    let blocked = db
        .create_task("test", "Blocked", None, 2, "a")
        .await
        .unwrap();
    let free = db.create_task("test", "Free", None, 1, "a").await.unwrap();

    db.add_dependency(&blocked.id, &blocker.id, "blocks")
        .await
        .unwrap();

    let ready = db.ready_tasks().await.unwrap();
    let ids: Vec<&str> = ready.iter().map(|t| t.id.as_str()).collect();

    assert!(ids.contains(&blocker.id.as_str()));
    assert!(ids.contains(&free.id.as_str()));
    assert!(!ids.contains(&blocked.id.as_str()));
}

#[tokio::test]
async fn completing_blocker_unblocks_dependent() {
    let (db, _dir) = temp_db().await;
    let blocker = db
        .create_task("test", "Blocker", None, 0, "a")
        .await
        .unwrap();
    let blocked = db
        .create_task("test", "Blocked", None, 0, "a")
        .await
        .unwrap();

    db.add_dependency(&blocked.id, &blocker.id, "blocks")
        .await
        .unwrap();
    assert!(
        !db.ready_tasks()
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == blocked.id)
    );

    db.close_task(&blocker.id, "a").await.unwrap();
    assert!(
        db.ready_tasks()
            .await
            .unwrap()
            .iter()
            .any(|t| t.id == blocked.id)
    );
}

#[tokio::test]
async fn chain_dependency_unblock() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("test", "A", None, 0, "x").await.unwrap();
    let b = db.create_task("test", "B", None, 0, "x").await.unwrap();
    let c = db.create_task("test", "C", None, 0, "x").await.unwrap();

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
    let task = db.create_task("test", "Real", None, 0, "a").await.unwrap();

    assert!(
        db.add_dependency(&task.id, "lt-fake", "blocks")
            .await
            .is_err()
    );
    assert!(
        db.add_dependency("lt-fake", &task.id, "blocks")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn remove_dependency_unblocks() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("test", "A", None, 0, "x").await.unwrap();
    let b = db.create_task("test", "B", None, 0, "x").await.unwrap();

    db.add_dependency(&b.id, &a.id, "blocks").await.unwrap();
    assert!(!db.ready_tasks().await.unwrap().iter().any(|t| t.id == b.id));

    db.remove_dependency(&b.id, &a.id).await.unwrap();
    assert!(db.ready_tasks().await.unwrap().iter().any(|t| t.id == b.id));
}

#[tokio::test]
async fn get_dependencies_returns_deps() {
    let (db, _dir) = temp_db().await;
    let a = db.create_task("test", "A", None, 0, "x").await.unwrap();
    let b = db.create_task("test", "B", None, 0, "x").await.unwrap();

    db.add_dependency(&b.id, &a.id, "blocks").await.unwrap();

    let deps = db.get_dependencies(&b.id).await.unwrap();
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].depends_on, a.id);
    assert_eq!(deps[0].dep_type, "blocks");
}

#[tokio::test]
async fn get_reverse_dependencies_returns_dependents() {
    let (db, _dir) = temp_db().await;
    let blocker = db.create_task("test", "A", None, 0, "x").await.unwrap();
    let dependent = db.create_task("test", "B", None, 0, "x").await.unwrap();

    db.add_dependency(&dependent.id, &blocker.id, "blocks")
        .await
        .unwrap();

    let deps = db.get_reverse_dependencies(&blocker.id).await.unwrap();
    assert_eq!(deps.len(), 1);
    assert_eq!(deps[0].task_id, dependent.id);
    assert_eq!(deps[0].depends_on, blocker.id);
}

#[tokio::test]
async fn delete_task_removes_related_rows() {
    let (db, _dir) = temp_db().await;
    let blocker = db.create_task("test", "A", None, 0, "x").await.unwrap();
    let dependent = db.create_task("test", "B", None, 0, "x").await.unwrap();

    db.add_dependency(&dependent.id, &blocker.id, "blocks")
        .await
        .unwrap();
    db.add_comment(&blocker.id, "x", "note").await.unwrap();
    db.delete_task(&blocker.id).await.unwrap();

    assert!(db.get_task(&blocker.id).await.is_err());
    assert!(
        db.get_reverse_dependencies(&blocker.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(db.get_dependencies(&dependent.id).await.unwrap().is_empty());
    assert!(db.get_events(&blocker.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn events_track_full_lifecycle() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Tracked", None, 1, "creator")
        .await
        .unwrap();

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
    let task = db
        .create_task("test", "Before", None, 1, "a")
        .await
        .unwrap();

    db.update_task(
        &task.id,
        TaskUpdates {
            priority: Some(3),
            title: Some("After"),
            ..Default::default()
        },
        "editor",
    )
    .await
    .unwrap();

    let events = db.get_events(&task.id).await.unwrap();
    assert_eq!(events.len(), 3); // created + priority + title

    let priority_ev = events
        .iter()
        .find(|e| e.field.as_deref() == Some("priority"))
        .unwrap();
    assert_eq!(priority_ev.old_value.as_deref(), Some("1"));
    assert_eq!(priority_ev.new_value.as_deref(), Some("3"));

    let title_ev = events
        .iter()
        .find(|e| e.field.as_deref() == Some("title"))
        .unwrap();
    assert_eq!(title_ev.old_value.as_deref(), Some("Before"));
    assert_eq!(title_ev.new_value.as_deref(), Some("After"));
}

#[tokio::test]
async fn add_and_get_comments() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Commentable", None, 0, "a")
        .await
        .unwrap();

    let c1 = db
        .add_comment(&task.id, "dev-0", "Blocked: need API key")
        .await
        .unwrap();
    let c2 = db
        .add_comment(&task.id, "manager", "Use env var SECRET_KEY")
        .await
        .unwrap();

    assert_eq!(c1.actor, "dev-0");
    assert_eq!(c1.content, "Blocked: need API key");
    assert_eq!(c2.actor, "manager");

    let comments = db.get_comments(&task.id).await.unwrap();
    assert_eq!(comments.len(), 2);
    assert_eq!(comments[0].id, c1.id);
    assert_eq!(comments[1].id, c2.id);
}

#[tokio::test]
async fn comments_empty_for_no_comments() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Silent", None, 0, "a")
        .await
        .unwrap();

    let comments = db.get_comments(&task.id).await.unwrap();
    assert!(comments.is_empty());
}

#[tokio::test]
async fn clear_assignee_sets_null() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Assigned", None, 0, "a")
        .await
        .unwrap();
    db.claim_task(&task.id, "dev-0").await.unwrap();

    assert_eq!(
        db.get_task(&task.id).await.unwrap().assignee.as_deref(),
        Some("dev-0")
    );

    db.clear_assignee(&task.id, "runtime").await.unwrap();
    let updated = db.get_task(&task.id).await.unwrap();
    assert!(updated.assignee.is_none());
}

#[tokio::test]
async fn clear_assignee_makes_task_ready_dispatchable() {
    let (db, _dir) = temp_db().await;
    let task = db.create_task("test", "Stuck", None, 0, "a").await.unwrap();
    db.claim_task(&task.id, "dev-0").await.unwrap();

    // Set back to ready but assignee still set
    let updates = TaskUpdates {
        status: Some("ready"),
        ..Default::default()
    };
    db.update_task(&task.id, updates, "runtime").await.unwrap();

    // ready_tasks requires assignee IS NULL — should NOT find it yet
    let ready = db.ready_tasks().await.unwrap();
    assert!(!ready.iter().any(|t| t.id == task.id));

    // After clearing assignee, ready_tasks should find it
    db.clear_assignee(&task.id, "runtime").await.unwrap();
    let ready = db.ready_tasks().await.unwrap();
    assert!(ready.iter().any(|t| t.id == task.id));
}

#[tokio::test]
async fn open_migrates_database_without_target_branch() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tasks (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT,
                status TEXT NOT NULL DEFAULT 'pending', priority INTEGER NOT NULL DEFAULT 0,
                assignee TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE dependencies (
                task_id TEXT NOT NULL REFERENCES tasks(id),
                depends_on TEXT NOT NULL REFERENCES tasks(id),
                dep_type TEXT NOT NULL DEFAULT 'blocks',
                PRIMARY KEY (task_id, depends_on)
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
                actor TEXT NOT NULL, action TEXT NOT NULL, field TEXT,
                old_value TEXT, new_value TEXT, timestamp TEXT NOT NULL
            );
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
                actor TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL
            );
            ",
        )
        .unwrap();
    }

    let db = Database::open(&path).await.unwrap();
    let task = db
        .create_task_with_branch("test", "Migrated", None, 0, "agent", Some("branch"))
        .await
        .unwrap();

    assert_eq!(task.target_branch.as_deref(), Some("branch"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_across_connections_have_one_winner() {
    // Two handles on one file stand in for two processes sharing the database.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("shared.db");
    let first = std::sync::Arc::new(Database::open(&path).await.unwrap());
    let second = std::sync::Arc::new(Database::open(&path).await.unwrap());
    let task = first
        .create_task("test", "Contested", None, 0, "a")
        .await
        .unwrap();

    let mut claims = Vec::new();
    for n in 0..16 {
        let db = if n % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let id = task.id.clone();
        claims.push(tokio::spawn(async move {
            db.claim_task(&id, &format!("agent-{n}")).await
        }));
    }
    let mut winners = 0;
    for claim in claims {
        match claim.await.unwrap() {
            Ok(()) => winners += 1,
            Err(error) => assert!(error.downcast_ref::<Conflict>().is_some(), "{error}"),
        }
    }

    assert_eq!(winners, 1);
    let claimed_events = first
        .get_events(&task.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.action == "claimed")
        .count();
    assert_eq!(claimed_events, 1);
}

#[tokio::test]
async fn claim_missing_task_is_not_found() {
    let (db, _dir) = temp_db().await;
    let error = db.claim_task("lt-none", "a").await.unwrap_err();
    assert!(error.downcast_ref::<NotFound>().is_some());
}

async fn set_due(db: &Database, id: &str, due_at: &str) {
    let updates = TaskUpdates {
        due_at: Some(due_at),
        ..Default::default()
    };
    db.update_task(id, updates, "agent-1").await.unwrap();
}

#[tokio::test]
async fn due_tasks_lists_only_open_undelivered_tasks_past_their_due_time() {
    let (db, _dir) = temp_db().await;
    let due = db
        .create_task("test", "Due", None, 0, "agent-1")
        .await
        .unwrap();
    let future = db
        .create_task("test", "Future", None, 0, "agent-1")
        .await
        .unwrap();
    let closed = db
        .create_task("test", "Closed", None, 0, "agent-1")
        .await
        .unwrap();
    let delivered = db
        .create_task("test", "Delivered", None, 0, "agent-1")
        .await
        .unwrap();
    db.create_task("test", "No due time", None, 0, "agent-1")
        .await
        .unwrap();
    set_due(&db, &due.id, "2026-11-15T09:00:00-06:00").await;
    set_due(&db, &future.id, "2026-11-16T09:00:00Z").await;
    set_due(&db, &closed.id, "2026-11-15T10:00:00Z").await;
    set_due(&db, &delivered.id, "2026-11-15T11:00:00Z").await;
    db.close_task(&closed.id, "agent-1").await.unwrap();
    db.mark_delivered(&delivered.id, "2026-11-15T11:00:00Z", "supervisor")
        .await
        .unwrap();

    let listed = db.due_tasks("2026-11-15T16:00:00Z").await.unwrap();

    let ids: Vec<&str> = listed.iter().map(|task| task.id.as_str()).collect();
    assert_eq!(ids, vec![due.id.as_str()]);
    assert_eq!(listed[0].due_at.as_deref(), Some("2026-11-15T15:00:00Z"));
}

#[tokio::test]
async fn invalid_due_time_is_rejected_without_change() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Reminder", None, 0, "agent-1")
        .await
        .unwrap();
    let updates = TaskUpdates {
        due_at: Some("tomorrow 9am"),
        ..Default::default()
    };

    let error = db
        .update_task(&task.id, updates, "agent-1")
        .await
        .unwrap_err();

    assert!(error.downcast_ref::<llm_tasks::db::Invalid>().is_some());
    assert!(db.get_task(&task.id).await.unwrap().due_at.is_none());
}

#[tokio::test]
async fn rescheduling_or_reopening_clears_delivery() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Reminder", None, 0, "agent-1")
        .await
        .unwrap();
    set_due(&db, &task.id, "2026-11-15T09:00:00Z").await;
    db.mark_delivered(&task.id, "2026-11-15T09:00:00Z", "supervisor")
        .await
        .unwrap();
    assert!(db.get_task(&task.id).await.unwrap().delivered_at.is_some());

    set_due(&db, &task.id, "2026-11-15T12:00:00Z").await;
    let rescheduled = db.get_task(&task.id).await.unwrap();
    assert!(rescheduled.delivered_at.is_none());
    assert_eq!(db.due_tasks("2026-11-15T13:00:00Z").await.unwrap().len(), 1);

    db.mark_delivered(&task.id, "2026-11-15T12:00:00Z", "supervisor")
        .await
        .unwrap();
    db.close_task(&task.id, "agent-1").await.unwrap();
    let reopen = TaskUpdates {
        status: Some("pending"),
        ..Default::default()
    };
    db.update_task(&task.id, reopen, "agent-1").await.unwrap();
    assert!(db.get_task(&task.id).await.unwrap().delivered_at.is_none());
}

#[tokio::test]
async fn mark_delivered_rejects_stale_due_time_and_double_delivery() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Reminder", None, 0, "agent-1")
        .await
        .unwrap();
    set_due(&db, &task.id, "2026-11-15T09:00:00Z").await;

    let stale = db
        .mark_delivered(&task.id, "2026-11-14T09:00:00Z", "supervisor")
        .await
        .unwrap_err();
    assert!(stale.downcast_ref::<Conflict>().is_some());

    db.mark_delivered(&task.id, "2026-11-15T09:00:00Z", "supervisor")
        .await
        .unwrap();
    let again = db
        .mark_delivered(&task.id, "2026-11-15T09:00:00Z", "supervisor")
        .await
        .unwrap_err();
    assert!(again.downcast_ref::<Conflict>().is_some());
    let missing = db
        .mark_delivered("lt-missing", "2026-11-15T09:00:00Z", "supervisor")
        .await
        .unwrap_err();
    assert!(missing.downcast_ref::<NotFound>().is_some());

    let events = db.get_events(&task.id).await.unwrap();
    let delivered = events.iter().find(|e| e.action == "delivered").unwrap();
    assert_eq!(delivered.actor, "supervisor");
    assert_eq!(delivered.new_value.as_deref(), Some("2026-11-15T09:00:00Z"));
}

fn question(text: &str, options: &[&str]) -> llm_tasks::questions::Question {
    llm_tasks::questions::Question {
        question: text.to_owned(),
        header: None,
        options: options
            .iter()
            .map(|label| llm_tasks::questions::QuestionOption {
                label: (*label).to_owned(),
                description: None,
            })
            .collect(),
        multi_select: false,
    }
}

fn answers(pairs: &[(&str, &str)]) -> llm_tasks::questions::Answers {
    pairs
        .iter()
        .map(|(q, a)| ((*q).to_owned(), (*a).to_owned()))
        .collect()
}

#[tokio::test]
async fn unanswered_questions_keep_a_task_out_of_ready_until_answered() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Migrate DB", None, 1, "agent-1")
        .await
        .unwrap();
    let asked = db
        .ask_questions(
            &task.id,
            vec![
                question("Which engine?", &["Postgres", "SQLite"]),
                question("Deadline?", &[]),
            ],
            "agent-1",
        )
        .await
        .unwrap();
    assert!(db.ready_tasks().await.unwrap().is_empty());

    let partial = db
        .answer_questions(
            asked.id,
            answers(&[("Which engine?", "SQLite")]),
            "orchestrator",
        )
        .await
        .unwrap_err();
    assert!(partial.downcast_ref::<llm_tasks::db::Invalid>().is_some());
    let full = answers(&[("Which engine?", "SQLite"), ("Deadline?", "Friday")]);
    db.answer_questions(asked.id, full.clone(), "orchestrator")
        .await
        .unwrap();

    let ready: Vec<String> = db
        .ready_tasks()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.id)
        .collect();
    assert_eq!(ready, vec![task.id.clone()]);
    let sets = db.list_questions(&task.id).await.unwrap();
    assert_eq!(sets[0].answers.as_ref(), Some(&full));
    assert_eq!(sets[0].answered_by.as_deref(), Some("orchestrator"));
    let again = db
        .answer_questions(asked.id, full, "orchestrator")
        .await
        .unwrap_err();
    assert!(again.downcast_ref::<Conflict>().is_some());
    let actions: Vec<String> = db
        .get_events(&task.id)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.action)
        .collect();
    assert!(actions.contains(&"asked".to_owned()) && actions.contains(&"answered".to_owned()));
}

#[tokio::test]
async fn malformed_question_sets_are_rejected() {
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Task", None, 0, "agent-1")
        .await
        .unwrap();
    let cases = vec![
        vec![],
        vec![question("One option?", &["Only"])],
        vec![question("Duplicate labels?", &["A", "A"])],
        vec![question("Same", &[]), question("Same", &[])],
        vec![question(" ", &[])],
    ];
    for questions in cases {
        let error = db
            .ask_questions(&task.id, questions, "agent-1")
            .await
            .unwrap_err();
        assert!(
            error.downcast_ref::<llm_tasks::db::Invalid>().is_some(),
            "{error}"
        );
    }
    assert!(db.list_questions(&task.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn reminders_are_not_ready_before_their_due_time() {
    let (db, _dir) = temp_db().await;
    let future = db
        .create_task("test", "Later", None, 0, "agent-1")
        .await
        .unwrap();
    let past = db
        .create_task("test", "Overdue", None, 0, "agent-1")
        .await
        .unwrap();
    set_due(&db, &future.id, "2099-01-01T00:00:00Z").await;
    set_due(&db, &past.id, "2020-01-01T00:00:00Z").await;

    let ready: Vec<String> = db
        .ready_tasks()
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.id)
        .collect();

    assert_eq!(ready, vec![past.id]);
}

#[tokio::test]
async fn attachments_round_trip_and_respect_the_size_cap() {
    use llm_tasks::attachments::{MAX_ATTACHMENT_BYTES, NewAttachment};
    let (db, _dir) = temp_db().await;
    let task = db
        .create_task("test", "Logs", None, 0, "agent-1")
        .await
        .unwrap();
    let data = b"line 1\nline 2\n";
    let file = NewAttachment {
        name: "build.log",
        mime_type: "text/plain",
        data,
    };
    let attached = db.add_attachment(&task.id, file, "agent-1").await.unwrap();

    let (meta, bytes) = db.get_attachment(attached.id).await.unwrap();
    assert_eq!(bytes, data);
    assert_eq!(meta.name, "build.log");
    assert_eq!(meta.size, data.len() as i64);
    assert_eq!(db.list_attachments(&task.id).await.unwrap(), vec![meta]);

    let big = vec![0u8; MAX_ATTACHMENT_BYTES + 1];
    let oversized = NewAttachment {
        name: "big.bin",
        mime_type: "application/octet-stream",
        data: &big,
    };
    let error = db
        .add_attachment(&task.id, oversized, "agent-1")
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<llm_tasks::db::Invalid>().is_some());
    let missing = db.get_attachment(9999).await.unwrap_err();
    assert!(missing.downcast_ref::<NotFound>().is_some());
    assert_eq!(db.list_attachments(&task.id).await.unwrap().len(), 1);
}

/// A database from before projects, holding tasks, comments and a dependency.
fn legacy_db_with_tasks(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT,
            status TEXT NOT NULL DEFAULT 'pending', priority INTEGER NOT NULL DEFAULT 0,
            assignee TEXT, target_branch TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            due_at TEXT, delivered_at TEXT
        );
        CREATE TABLE dependencies (
            task_id TEXT NOT NULL REFERENCES tasks(id), depends_on TEXT NOT NULL REFERENCES tasks(id),
            dep_type TEXT NOT NULL DEFAULT 'blocks', PRIMARY KEY (task_id, depends_on)
        );
        INSERT INTO tasks (id, title, status, priority, created_at, updated_at, due_at)
            VALUES ('lt-0001', 'Old work', 'pending', 2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL),
                   ('lt-0002', 'Old reminder', 'pending', 0, '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z', '2026-12-01T09:00:00Z');
        INSERT INTO dependencies (task_id, depends_on) VALUES ('lt-0002', 'lt-0001');
        ",
    )
    .unwrap();
}

#[tokio::test]
async fn legacy_tasks_block_open_until_every_task_has_a_project() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("legacy.db");
    legacy_db_with_tasks(&path);

    let error = Database::open(&path).await.err().unwrap().to_string();
    assert!(error.contains("2 tasks have no project"), "{error}");

    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "UPDATE tasks SET project = 'wow-ui-sim' WHERE id = 'lt-0001';
             UPDATE tasks SET project = 'personal' WHERE id = 'lt-0002';",
        )
        .unwrap();
    let db = Database::open(&path).await.unwrap();

    let old = db.get_task("lt-0001").await.unwrap();
    assert_eq!((old.project.as_str(), old.priority), ("wow-ui-sim", 2));
    let reminder = db.get_task("lt-0002").await.unwrap();
    assert_eq!(reminder.project, "personal");
    assert_eq!(reminder.due_at.as_deref(), Some("2026-12-01T09:00:00Z"));
    assert_eq!(
        db.get_dependencies("lt-0002").await.unwrap()[0].depends_on,
        "lt-0001"
    );
    drop(db);
    let null_insert = rusqlite::Connection::open(&path).unwrap().execute(
        "INSERT INTO tasks (id, title, created_at, updated_at) VALUES ('lt-0003', 'x', 'a', 'a')",
        [],
    );
    assert!(null_insert.unwrap_err().to_string().contains("NOT NULL"));
}

#[tokio::test]
async fn projects_are_validated_filtered_and_movable() {
    let (db, _dir) = temp_db().await;
    for bad in ["", "WoW", "wow ui", "-lead", &"x".repeat(41)] {
        let error = db.create_task(bad, "Nope", None, 0, "a").await.unwrap_err();
        assert!(
            error.downcast_ref::<llm_tasks::db::Invalid>().is_some(),
            "{bad:?}"
        );
    }
    let sim = db
        .create_task("wow-ui-sim", "Tooltip", None, 0, "a")
        .await
        .unwrap();
    db.create_task("world-of-osso", "AV window", None, 0, "a")
        .await
        .unwrap();

    let only_sim = db.list_tasks(None, None, Some("wow-ui-sim")).await.unwrap();
    assert_eq!(
        only_sim.iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
        [sim.id.as_str()]
    );

    let invalid = TaskUpdates {
        project: Some("Bad Name"),
        ..Default::default()
    };
    assert!(db.update_task(&sim.id, invalid, "a").await.is_err());
    let moved = TaskUpdates {
        project: Some("world-of-osso"),
        ..Default::default()
    };
    db.update_task(&sim.id, moved, "mover").await.unwrap();
    assert_eq!(
        db.list_tasks(None, None, Some("world-of-osso"))
            .await
            .unwrap()
            .len(),
        2
    );
    let event = db.get_events(&sim.id).await.unwrap().pop().unwrap();
    assert_eq!(
        (
            event.actor.as_str(),
            event.field.as_deref(),
            event.old_value.as_deref(),
            event.new_value.as_deref()
        ),
        (
            "mover",
            Some("project"),
            Some("wow-ui-sim"),
            Some("world-of-osso")
        )
    );
}
