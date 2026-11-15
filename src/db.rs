use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

use crate::id;

pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

/// The requested task does not exist. Carried inside `anyhow::Error`; the HTTP
/// layer maps it to 404 and the remote client maps 404 back to it.
#[derive(Debug)]
pub struct NotFound(pub String);

/// The task exists but is not in a state that allows the operation (HTTP 409).
#[derive(Debug)]
pub struct Conflict(pub String);

impl std::fmt::Display for NotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Display for Conflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The request carries an invalid value (HTTP 400).
#[derive(Debug)]
pub struct Invalid(pub String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NotFound {}
impl std::error::Error for Conflict {}
impl std::error::Error for Invalid {}

/// Columns read by `row_to_task`, in order.
const TASK_COLUMNS: &str = "id, title, description, status, priority, assignee, target_branch, created_at, updated_at, due_at, delivered_at";

/// Statuses that end a task; closed tasks are never due.
const CLOSED_STATUSES: &str = "('completed', 'done')";

/// Normalize a due time with an explicit offset to RFC 3339 UTC (`...Z`, seconds).
pub fn normalize_due_at(value: &str) -> Result<String> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
        Invalid(format!(
            "Invalid due time {value:?}: use RFC 3339 with an offset, e.g. 2026-11-15T09:00:00-06:00"
        ))
    })?;
    Ok(parsed
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: u8,
    pub assignee: Option<String>,
    pub target_branch: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// When the task is due (RFC 3339 UTC); a task with a due time is a reminder.
    #[serde(default)]
    pub due_at: Option<String>,
    /// When the current due time was delivered; cleared by rescheduling or reopening.
    #[serde(default)]
    pub delivered_at: Option<String>,
}

#[derive(Debug, Default)]
pub struct TaskUpdates<'a> {
    pub status: Option<&'a str>,
    pub priority: Option<u8>,
    pub title: Option<&'a str>,
    pub description: Option<&'a str>,
    pub assignee: Option<&'a str>,
    pub target_branch: Option<&'a str>,
    pub due_at: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Dependency {
    pub task_id: String,
    pub depends_on: String,
    pub dep_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Comment {
    pub id: i64,
    pub task_id: String,
    pub actor: String,
    pub content: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Event {
    pub id: i64,
    pub task_id: String,
    pub actor: String,
    pub action: String,
    pub field: Option<String>,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    pub timestamp: String,
}

impl Database {
    pub async fn open(path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path)?;
            conn.pragma_update(None, "journal_mode", "wal")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            create_schema(&conn)?;
            Ok(conn)
        })
        .await??;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn create_task(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        self.create_task_with_branch(title, desc, priority, actor, None)
            .await
    }

    pub async fn create_task_with_branch(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
        target_branch: Option<&str>,
    ) -> Result<Task> {
        let id = id::generate();
        let now = now_iso();
        let conn = self.conn.clone();
        let id2 = id.clone();
        let title = title.to_string();
        let desc = desc.map(|s| s.to_string());
        let actor = actor.to_string();
        let target_branch = target_branch.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO tasks (id, title, description, status, priority, assignee, target_branch, created_at, updated_at) VALUES (?1, ?2, ?3, 'pending', ?4, NULL, ?5, ?6, ?6)",
                params![&id2, &title, desc.as_deref().unwrap_or(""), priority, target_branch.as_deref(), &now],
            )?;
            record_event(&c, &id2, &actor, "created", None, None, None)?;
            Ok(())
        })
        .await??;
        self.get_task(&id).await
    }

    pub async fn get_task(&self, id: &str) -> Result<Task> {
        let conn = self.conn.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare(&format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id = ?1"))?;
            let mut rows = stmt.query(params![&id])?;
            match rows.next()? {
                Some(row) => Ok(row_to_task(row)),
                None => Err(NotFound(format!("Task not found: {}", id)).into()),
            }
        })
        .await?
    }

    pub async fn list_tasks(
        &self,
        status: Option<&str>,
        assignee: Option<&str>,
    ) -> Result<Vec<Task>> {
        let (sql, param_values) = build_list_query(status, assignee);
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let params: Vec<&dyn rusqlite::types::ToSql> = param_values
                .iter()
                .map(|s| s as &dyn rusqlite::types::ToSql)
                .collect();
            let mut stmt = c.prepare(&sql)?;
            let mut rows = stmt.query(params.as_slice())?;
            collect_tasks(&mut rows)
        })
        .await?
    }

    pub async fn ready_tasks(&self) -> Result<Vec<Task>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let sql = "SELECT t.id, t.title, t.description, t.status, t.priority, t.assignee, t.target_branch, t.created_at, t.updated_at, t.due_at, t.delivered_at FROM tasks t WHERE t.status IN ('pending', 'ready') AND t.assignee IS NULL AND t.id NOT IN (SELECT d.task_id FROM dependencies d JOIN tasks bt ON d.depends_on = bt.id WHERE bt.status NOT IN ('completed', 'done')) ORDER BY t.priority DESC, t.created_at ASC";
            let mut stmt = c.prepare(sql)?;
            let mut rows = stmt.query([])?;
            collect_tasks(&mut rows)
        })
        .await?
    }

    /// Claim in one conditional UPDATE inside a transaction, so concurrent
    /// claimers (in-process or separate processes on the same file) get exactly
    /// one winner; the losers see `Conflict`.
    pub async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut c = conn.lock().unwrap();
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let previous: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT status, assignee FROM tasks WHERE id = ?1",
                    params![&id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((status, assignee)) = previous else {
                return Err(NotFound(format!("Task not found: {}", id)).into());
            };
            let claimed = tx.execute(
                "UPDATE tasks SET assignee = ?1, status = 'in_progress', updated_at = ?2 WHERE id = ?3 AND (status = 'pending' OR status = 'ready') AND assignee IS NULL",
                params![&actor, &now, &id],
            )?;
            if claimed == 0 {
                let reason = match assignee {
                    Some(owner) => format!("already assigned to {}", owner),
                    None => format!("status is {}", status),
                };
                return Err(Conflict(format!("Cannot claim task {}: {}", id, reason)).into());
            }
            record_event(&tx, &id, &actor, "claimed", Some("assignee"), None, Some(&actor))?;
            record_event(&tx, &id, &actor, "updated", Some("status"), Some(&status), Some("in_progress"))?;
            tx.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn update_task(&self, id: &str, updates: TaskUpdates<'_>, actor: &str) -> Result<()> {
        let due_at = match updates.due_at {
            Some(value) if !value.is_empty() => Some(normalize_due_at(value)?),
            other => other.map(str::to_owned),
        };
        let task = self.get_task(id).await?;
        let changes = field_changes(&task, &updates, due_at);
        let resets_delivery = updates.status == Some("pending")
            || changes
                .iter()
                .any(|change| change.field == "due_at" && change.is_change());
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            for change in &changes {
                apply_field_update(&c, &id, &actor, &now, change)?;
            }
            if resets_delivery {
                c.execute(
                    "UPDATE tasks SET delivered_at = NULL WHERE id = ?1",
                    params![&id],
                )?;
            }
            Ok(())
        })
        .await?
    }

    /// Open tasks whose due time has passed and whose current due time is not yet delivered.
    pub async fn due_tasks(&self, now: &str) -> Result<Vec<Task>> {
        let conn = self.conn.clone();
        let now = now.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let sql = format!(
                "SELECT {TASK_COLUMNS} FROM tasks WHERE status NOT IN {CLOSED_STATUSES} AND due_at IS NOT NULL AND due_at <= ?1 AND delivered_at IS NULL ORDER BY due_at, id"
            );
            let mut stmt = c.prepare(&sql)?;
            let mut rows = stmt.query(params![&now])?;
            collect_tasks(&mut rows)
        })
        .await?
    }

    /// Acknowledge delivery of the task's due time `due_at`. Conflict when the task
    /// was rescheduled, closed or already delivered since the caller read it.
    pub async fn mark_delivered(&self, id: &str, due_at: &str, actor: &str) -> Result<()> {
        self.get_task(id).await?;
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let due_at = due_at.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            let sql = format!(
                "UPDATE tasks SET delivered_at = ?1 WHERE id = ?2 AND due_at = ?3 AND delivered_at IS NULL AND status NOT IN {CLOSED_STATUSES}"
            );
            if c.execute(&sql, params![&now, &id, &due_at])? == 0 {
                return Err(Conflict(format!(
                    "Task {id} has no undelivered due time {due_at}"
                ))
                .into());
            }
            record_event(&c, &id, &actor, "delivered", Some("due_at"), None, Some(&due_at))
        })
        .await?
    }

    /// Set assignee to NULL (not empty string).
    pub async fn clear_assignee(&self, id: &str, actor: &str) -> Result<()> {
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "UPDATE tasks SET assignee = NULL, updated_at = ?1 WHERE id = ?2",
                params![&now, &id],
            )?;
            record_event(&c, &id, &actor, "updated", Some("assignee"), None, None)?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn add_comment(&self, task_id: &str, actor: &str, content: &str) -> Result<Comment> {
        let now = now_iso();
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        let actor = actor.to_string();
        let content = content.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO comments (task_id, actor, content, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![&task_id, &actor, &content, &now],
            )?;
            let id = c.last_insert_rowid();
            record_event(&c, &task_id, &actor, "commented", None, None, Some(&id.to_string()))?;
            Ok(Comment { id, task_id, actor, content, created_at: now })
        })
        .await?
    }

    pub async fn get_comments(&self, task_id: &str) -> Result<Vec<Comment>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare(
                "SELECT id, task_id, actor, content, created_at FROM comments WHERE task_id = ?1 ORDER BY id",
            )?;
            let rows = stmt.query_map(params![&task_id], |row| {
                Ok(Comment {
                    id: row.get(0)?,
                    task_id: row.get(1)?,
                    actor: row.get(2)?,
                    content: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await?
    }

    pub async fn close_task(&self, id: &str, actor: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "UPDATE tasks SET status = 'completed', updated_at = ?1 WHERE id = ?2",
                params![&now, &id],
            )?;
            record_event(
                &c,
                &id,
                &actor,
                "closed",
                Some("status"),
                Some(&task.status),
                Some("completed"),
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn add_dependency(
        &self,
        task_id: &str,
        depends_on: &str,
        dep_type: &str,
    ) -> Result<()> {
        self.get_task(task_id).await?;
        self.get_task(depends_on).await?;
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        let depends_on = depends_on.to_string();
        let dep_type = dep_type.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO dependencies (task_id, depends_on, dep_type) VALUES (?1, ?2, ?3)",
                params![&task_id, &depends_on, &dep_type],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn delete_task(&self, id: &str) -> Result<()> {
        let conn = self.conn.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute("DELETE FROM events WHERE task_id = ?1", params![&id])?;
            c.execute("DELETE FROM comments WHERE task_id = ?1", params![&id])?;
            c.execute(
                "DELETE FROM dependencies WHERE task_id = ?1 OR depends_on = ?1",
                params![&id],
            )?;
            c.execute("DELETE FROM tasks WHERE id = ?1", params![&id])?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn remove_dependency(&self, task_id: &str, depends_on: &str) -> Result<()> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        let depends_on = depends_on.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "DELETE FROM dependencies WHERE task_id = ?1 AND depends_on = ?2",
                params![&task_id, &depends_on],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn get_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        self.query_dependencies(task_id, DependencyColumn::TaskId)
            .await
    }

    pub async fn get_reverse_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        self.query_dependencies(task_id, DependencyColumn::DependsOn)
            .await
    }

    async fn query_dependencies(
        &self,
        task_id: &str,
        column: DependencyColumn,
    ) -> Result<Vec<Dependency>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        let sql = column.select_sql();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare(sql)?;
            let rows = stmt.query_map(params![&task_id], row_to_dependency)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await?
    }

    /// Events on all tasks with `id > after`, oldest first: a cursor feed for watchers.
    pub async fn events_after(&self, after: i64) -> Result<Vec<Event>> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare("SELECT id, task_id, actor, action, field, old_value, new_value, timestamp FROM events WHERE id > ?1 ORDER BY id ASC")?;
            let rows = stmt.query_map(params![after], event_from_row)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await?
    }

    pub async fn get_events(&self, task_id: &str) -> Result<Vec<Event>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare("SELECT id, task_id, actor, action, field, old_value, new_value, timestamp FROM events WHERE task_id = ?1 ORDER BY id ASC")?;
            let rows = stmt.query_map(params![&task_id], event_from_row)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await?
    }
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT,
        status TEXT NOT NULL DEFAULT 'pending', priority INTEGER NOT NULL DEFAULT 0,
        assignee TEXT, target_branch TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        due_at TEXT, delivered_at TEXT
    );
    CREATE TABLE IF NOT EXISTS dependencies (
        task_id TEXT NOT NULL REFERENCES tasks(id),
        depends_on TEXT NOT NULL REFERENCES tasks(id),
        dep_type TEXT NOT NULL DEFAULT 'blocks',
        PRIMARY KEY (task_id, depends_on)
    );
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
        actor TEXT NOT NULL, action TEXT NOT NULL, field TEXT,
        old_value TEXT, new_value TEXT, timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL REFERENCES tasks(id),
        actor TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL
    );
";

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA)?;
    migrate(conn)?;
    Ok(())
}

fn migrate(conn: &Connection) -> Result<()> {
    // Add target_branch column if missing (existing DBs created before this field).
    let has_col: bool = conn
        .prepare("SELECT target_branch FROM tasks LIMIT 0")
        .is_ok();
    if !has_col {
        conn.execute_batch("ALTER TABLE tasks ADD COLUMN target_branch TEXT")?;
    }
    if conn.prepare("SELECT due_at FROM tasks LIMIT 0").is_err() {
        conn.execute_batch(
            "ALTER TABLE tasks ADD COLUMN due_at TEXT; ALTER TABLE tasks ADD COLUMN delivered_at TEXT;",
        )?;
    }
    Ok(())
}

fn event_from_row(row: &rusqlite::Row) -> rusqlite::Result<Event> {
    Ok(Event {
        id: row.get(0)?,
        task_id: row.get(1)?,
        actor: row.get(2)?,
        action: row.get(3)?,
        field: row.get(4)?,
        old_value: row.get(5)?,
        new_value: row.get(6)?,
        timestamp: row.get(7)?,
    })
}

fn record_event(
    conn: &Connection,
    task_id: &str,
    actor: &str,
    action: &str,
    field: Option<&str>,
    old: Option<&str>,
    new: Option<&str>,
) -> Result<()> {
    let now = now_iso();
    conn.execute(
        "INSERT INTO events (task_id, actor, action, field, old_value, new_value, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![task_id, actor, action, field.unwrap_or(""), old.unwrap_or(""), new.unwrap_or(""), &now],
    )?;
    Ok(())
}

/// One field of an update: the stored value and the requested one (None = unchanged).
struct FieldChange {
    field: &'static str,
    old: String,
    new: Option<String>,
}

impl FieldChange {
    fn new(field: &'static str, old: Option<&str>, new: Option<&str>) -> Self {
        Self {
            field,
            old: old.unwrap_or("").to_owned(),
            new: new.map(str::to_owned),
        }
    }

    fn is_change(&self) -> bool {
        self.new.as_ref().is_some_and(|new| *new != self.old)
    }
}

fn field_changes(
    task: &Task,
    updates: &TaskUpdates<'_>,
    due_at: Option<String>,
) -> Vec<FieldChange> {
    let priority = task.priority.to_string();
    let new_priority = updates.priority.map(|p| p.to_string());
    vec![
        FieldChange::new("status", Some(&task.status), updates.status),
        FieldChange::new("priority", Some(&priority), new_priority.as_deref()),
        FieldChange::new("title", Some(&task.title), updates.title),
        FieldChange::new(
            "description",
            task.description.as_deref(),
            updates.description,
        ),
        FieldChange::new("assignee", task.assignee.as_deref(), updates.assignee),
        FieldChange::new(
            "target_branch",
            task.target_branch.as_deref(),
            updates.target_branch,
        ),
        FieldChange::new("due_at", task.due_at.as_deref(), due_at.as_deref()),
    ]
}

fn apply_field_update(
    conn: &Connection,
    id: &str,
    actor: &str,
    now: &str,
    change: &FieldChange,
) -> Result<()> {
    if !change.is_change() {
        return Ok(());
    }
    let new_val = change.new.as_deref().unwrap_or("");
    let sql = format!(
        "UPDATE tasks SET {} = ?1, updated_at = ?2 WHERE id = ?3",
        change.field
    );
    // Empty string means "clear the field" → store as NULL
    let db_val: Option<&str> = if new_val.is_empty() {
        None
    } else {
        Some(new_val)
    };
    conn.execute(&sql, params![db_val, now, id])?;
    record_event(
        conn,
        id,
        actor,
        "updated",
        Some(change.field),
        Some(&change.old),
        Some(new_val),
    )?;
    Ok(())
}

fn build_list_query(status: Option<&str>, assignee: Option<&str>) -> (String, Vec<String>) {
    let mut sql = format!("SELECT {TASK_COLUMNS} FROM tasks");
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    if let Some(s) = status {
        params.push(s.to_string());
        conditions.push(format!("status = ?{}", params.len()));
    }
    if let Some(a) = assignee {
        params.push(a.to_string());
        conditions.push(format!("assignee = ?{}", params.len()));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY priority DESC, created_at ASC");
    (sql, params)
}

fn collect_tasks(rows: &mut rusqlite::Rows) -> Result<Vec<Task>> {
    let mut tasks = Vec::new();
    while let Some(row) = rows.next()? {
        tasks.push(row_to_task(row));
    }
    Ok(tasks)
}

fn row_to_dependency(row: &rusqlite::Row) -> rusqlite::Result<Dependency> {
    Ok(Dependency {
        task_id: row.get(0)?,
        depends_on: row.get(1)?,
        dep_type: row.get(2)?,
    })
}

fn row_to_task(row: &rusqlite::Row) -> Task {
    Task {
        id: row.get(0).unwrap_or_default(),
        title: row.get(1).unwrap_or_default(),
        description: row.get(2).ok(),
        status: row.get(3).unwrap_or_default(),
        priority: row.get::<_, i32>(4).unwrap_or(0) as u8,
        assignee: row.get(5).ok(),
        target_branch: row.get(6).ok(),
        created_at: row.get(7).unwrap_or_default(),
        updated_at: row.get(8).unwrap_or_default(),
        due_at: row.get(9).ok(),
        delivered_at: row.get(10).ok(),
    }
}

pub fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[derive(Clone, Copy)]
enum DependencyColumn {
    TaskId,
    DependsOn,
}

impl DependencyColumn {
    fn select_sql(self) -> &'static str {
        match self {
            Self::TaskId => {
                "SELECT task_id, depends_on, dep_type FROM dependencies WHERE task_id = ?1"
            }
            Self::DependsOn => {
                "SELECT task_id, depends_on, dep_type FROM dependencies WHERE depends_on = ?1"
            }
        }
    }
}
