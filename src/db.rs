use anyhow::{Result, bail};
use rusqlite::{Connection, params};
use serde::Serialize;
use std::sync::{Arc, Mutex};

use crate::id;

pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: u8,
    pub assignee: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Default)]
pub struct TaskUpdates<'a> {
    pub status: Option<&'a str>,
    pub priority: Option<u8>,
    pub title: Option<&'a str>,
    pub description: Option<&'a str>,
    pub assignee: Option<&'a str>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct Dependency {
    pub task_id: String,
    pub depends_on: String,
    pub dep_type: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
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
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub async fn create_task(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        let id = id::generate();
        let now = now_iso();
        let conn = self.conn.clone();
        let id2 = id.clone();
        let title = title.to_string();
        let desc = desc.map(|s| s.to_string());
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "INSERT INTO tasks (id, title, description, status, priority, assignee, created_at, updated_at) VALUES (?1, ?2, ?3, 'pending', ?4, NULL, ?5, ?5)",
                params![&id2, &title, desc.as_deref().unwrap_or(""), priority, &now],
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
            let mut stmt = c.prepare("SELECT id, title, description, status, priority, assignee, created_at, updated_at FROM tasks WHERE id = ?1")?;
            let mut rows = stmt.query(params![&id])?;
            match rows.next()? {
                Some(row) => Ok(row_to_task(row)),
                None => bail!("Task not found: {}", id),
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
            let params: Vec<&dyn rusqlite::types::ToSql> =
                param_values.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
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
            let sql = "SELECT t.id, t.title, t.description, t.status, t.priority, t.assignee, t.created_at, t.updated_at FROM tasks t WHERE t.status IN ('pending', 'ready') AND t.assignee IS NULL AND t.id NOT IN (SELECT d.task_id FROM dependencies d JOIN tasks bt ON d.depends_on = bt.id WHERE bt.status NOT IN ('completed', 'done')) ORDER BY t.priority DESC, t.created_at ASC";
            let mut stmt = c.prepare(sql)?;
            let mut rows = stmt.query([])?;
            collect_tasks(&mut rows)
        })
        .await?
    }

    pub async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        if task.status != "pending" && task.status != "ready" {
            bail!("Cannot claim task {}: status is {}", id, task.status);
        }
        if task.assignee.is_some() {
            bail!(
                "Cannot claim task {}: already assigned to {}",
                id,
                task.assignee.unwrap()
            );
        }
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            c.execute(
                "UPDATE tasks SET assignee = ?1, status = 'in_progress', updated_at = ?2 WHERE id = ?3 AND (status = 'pending' OR status = 'ready') AND assignee IS NULL",
                params![&actor, &now, &id],
            )?;
            record_event(&c, &id, &actor, "claimed", Some("assignee"), None, Some(&actor))?;
            record_event(&c, &id, &actor, "updated", Some("status"), Some("pending"), Some("in_progress"))?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn update_task(
        &self,
        id: &str,
        updates: TaskUpdates<'_>,
        actor: &str,
    ) -> Result<()> {
        let task = self.get_task(id).await?;
        let now = now_iso();
        let conn = self.conn.clone();
        let id = id.to_string();
        let actor = actor.to_string();
        let status = updates.status.map(|s| s.to_string());
        let priority = updates.priority;
        let title = updates.title.map(|s| s.to_string());
        let description = updates.description.map(|s| s.to_string());
        let assignee = updates.assignee.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            apply_task_fields(
                &c, &id, &actor, &now, &task,
                status.as_deref(), priority, title.as_deref(),
                description.as_deref(), assignee.as_deref(),
            )
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
            record_event(&c, &id, &actor, "closed", Some("status"), Some(&task.status), Some("completed"))?;
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
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare(
                "SELECT task_id, depends_on, dep_type FROM dependencies WHERE task_id = ?1",
            )?;
            let rows = stmt.query_map(params![&task_id], |row| {
                Ok(Dependency {
                    task_id: row.get(0)?,
                    depends_on: row.get(1)?,
                    dep_type: row.get(2)?,
                })
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await?
    }

    pub async fn get_reverse_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let mut stmt = c.prepare(
                "SELECT task_id, depends_on, dep_type FROM dependencies WHERE depends_on = ?1",
            )?;
            let rows = stmt.query_map(params![&task_id], |row| {
                Ok(Dependency {
                    task_id: row.get(0)?,
                    depends_on: row.get(1)?,
                    dep_type: row.get(2)?,
                })
            })?;
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
            let rows = stmt.query_map(params![&task_id], |row| {
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
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
        })
        .await?
    }
}

fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            assignee TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dependencies (
            task_id TEXT NOT NULL REFERENCES tasks(id),
            depends_on TEXT NOT NULL REFERENCES tasks(id),
            dep_type TEXT NOT NULL DEFAULT 'blocks',
            PRIMARY KEY (task_id, depends_on)
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL REFERENCES tasks(id),
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            field TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT NOT NULL
        );"
    )?;
    Ok(())
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

fn apply_task_fields(
    conn: &Connection,
    id: &str,
    actor: &str,
    now: &str,
    task: &Task,
    status: Option<&str>,
    priority: Option<u8>,
    title: Option<&str>,
    description: Option<&str>,
    assignee: Option<&str>,
) -> Result<()> {
    apply_field_update(conn, id, actor, now, "status", &task.status, status)?;
    let pri = task.priority.to_string();
    let new_pri = priority.map(|p| p.to_string());
    apply_field_update(conn, id, actor, now, "priority", &pri, new_pri.as_deref())?;
    apply_field_update(conn, id, actor, now, "title", &task.title, title)?;
    let desc = task.description.as_deref().unwrap_or("");
    apply_field_update(conn, id, actor, now, "description", desc, description)?;
    let asgn = task.assignee.as_deref().unwrap_or("");
    apply_field_update(conn, id, actor, now, "assignee", asgn, assignee)?;
    Ok(())
}

fn apply_field_update(
    conn: &Connection,
    id: &str,
    actor: &str,
    now: &str,
    field: &str,
    old: &str,
    new: Option<&str>,
) -> Result<()> {
    let Some(new_val) = new else { return Ok(()) };
    if new_val == old {
        return Ok(());
    }
    let sql = format!(
        "UPDATE tasks SET {} = ?1, updated_at = ?2 WHERE id = ?3",
        field
    );
    conn.execute(&sql, params![new_val, now, id])?;
    record_event(conn, id, actor, "updated", Some(field), Some(old), Some(new_val))?;
    Ok(())
}

fn build_list_query(status: Option<&str>, assignee: Option<&str>) -> (String, Vec<String>) {
    let mut sql = "SELECT id, title, description, status, priority, assignee, created_at, updated_at FROM tasks".to_string();
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

fn row_to_task(row: &rusqlite::Row) -> Task {
    Task {
        id: row.get(0).unwrap_or_default(),
        title: row.get(1).unwrap_or_default(),
        description: row.get(2).ok(),
        status: row.get(3).unwrap_or_default(),
        priority: row.get::<_, i32>(4).unwrap_or(0) as u8,
        assignee: row.get(5).ok(),
        created_at: row.get(6).unwrap_or_default(),
        updated_at: row.get(7).unwrap_or_default(),
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
