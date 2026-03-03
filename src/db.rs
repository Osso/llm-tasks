use anyhow::{bail, Result};
use serde::Serialize;
use turso::{Builder, Connection};

use crate::id;

pub struct Database {
    conn: Connection,
}

#[derive(Debug, Serialize, Clone)]
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

#[derive(Debug, Serialize, Clone)]
pub struct Dependency {
    pub task_id: String,
    pub depends_on: String,
    pub dep_type: String,
}

#[derive(Debug, Serialize, Clone)]
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
        let db = Builder::new_local(path.to_str().unwrap_or("tasks.db"))
            .build()
            .await?;
        let conn = db.connect()?;
        create_schema(&conn).await?;
        Ok(Self { conn })
    }

    pub async fn create_task(&self, title: &str, desc: Option<&str>, priority: u8, actor: &str) -> Result<Task> {
        let id = id::generate();
        let now = now_iso();
        self.conn.execute(
            "INSERT INTO tasks (id, title, description, status, priority, assignee, created_at, updated_at) VALUES (?1, ?2, ?3, 'pending', ?4, NULL, ?5, ?5)",
            [&id, title, desc.unwrap_or(""), &priority.to_string(), &now],
        ).await?;
        record_event(&self.conn, &id, actor, "created", None, None, None).await?;
        self.get_task(&id).await
    }

    pub async fn get_task(&self, id: &str) -> Result<Task> {
        let mut stmt = self.conn.prepare("SELECT id, title, description, status, priority, assignee, created_at, updated_at FROM tasks WHERE id = ?1").await?;
        let mut rows = stmt.query([id]).await?;
        match rows.next().await? {
            Some(row) => Ok(row_to_task(&row)),
            None => bail!("Task not found: {}", id),
        }
    }

    pub async fn list_tasks(&self, status: Option<&str>, assignee: Option<&str>) -> Result<Vec<Task>> {
        let (sql, params) = build_list_query(status, assignee);
        let values: Vec<turso::Value> = params.into_iter().map(turso::Value::Text).collect();
        let mut rows = self.conn.query(&sql, values).await?;
        collect_tasks(&mut rows).await
    }

    pub async fn ready_tasks(&self) -> Result<Vec<Task>> {
        let sql = "SELECT t.id, t.title, t.description, t.status, t.priority, t.assignee, t.created_at, t.updated_at FROM tasks t WHERE t.status = 'pending' AND t.id NOT IN (SELECT d.task_id FROM dependencies d JOIN tasks bt ON d.depends_on = bt.id WHERE bt.status != 'completed') ORDER BY t.priority DESC, t.created_at ASC";
        let mut stmt = self.conn.prepare(sql).await?;
        let mut rows = stmt.query(()).await?;
        collect_tasks(&mut rows).await
    }

    pub async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        if task.status != "pending" {
            bail!("Cannot claim task {}: status is {}", id, task.status);
        }
        if task.assignee.is_some() {
            bail!("Cannot claim task {}: already assigned to {}", id, task.assignee.unwrap());
        }
        let now = now_iso();
        self.conn.execute(
            "UPDATE tasks SET assignee = ?1, status = 'in_progress', updated_at = ?2 WHERE id = ?3 AND status = 'pending' AND assignee IS NULL",
            [actor, &now, id],
        ).await?;
        record_event(&self.conn, id, actor, "claimed", Some("assignee"), None, Some(actor)).await?;
        record_event(&self.conn, id, actor, "updated", Some("status"), Some("pending"), Some("in_progress")).await?;
        Ok(())
    }

    pub async fn update_task(&self, id: &str, status: Option<&str>, priority: Option<u8>, title: Option<&str>, desc: Option<&str>, actor: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        let now = now_iso();
        apply_field_update(&self.conn, id, actor, &now, "status", &task.status, status).await?;
        apply_field_update(&self.conn, id, actor, &now, "priority", &task.priority.to_string(), priority.map(|p| p.to_string()).as_deref()).await?;
        apply_field_update(&self.conn, id, actor, &now, "title", &task.title, title).await?;
        apply_field_update(&self.conn, id, actor, &now, "description", task.description.as_deref().unwrap_or(""), desc).await?;
        Ok(())
    }

    pub async fn close_task(&self, id: &str, actor: &str) -> Result<()> {
        let task = self.get_task(id).await?;
        let now = now_iso();
        self.conn.execute(
            "UPDATE tasks SET status = 'completed', updated_at = ?1 WHERE id = ?2",
            [&now, id],
        ).await?;
        record_event(&self.conn, id, actor, "closed", Some("status"), Some(&task.status), Some("completed")).await?;
        Ok(())
    }

    pub async fn add_dependency(&self, task_id: &str, depends_on: &str, dep_type: &str) -> Result<()> {
        self.get_task(task_id).await?;
        self.get_task(depends_on).await?;
        self.conn.execute(
            "INSERT INTO dependencies (task_id, depends_on, dep_type) VALUES (?1, ?2, ?3)",
            [task_id, depends_on, dep_type],
        ).await?;
        Ok(())
    }

    pub async fn remove_dependency(&self, task_id: &str, depends_on: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM dependencies WHERE task_id = ?1 AND depends_on = ?2",
            [task_id, depends_on],
        ).await?;
        Ok(())
    }

    pub async fn get_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        let mut stmt = self.conn.prepare("SELECT task_id, depends_on, dep_type FROM dependencies WHERE task_id = ?1").await?;
        let mut rows = stmt.query([task_id]).await?;
        let mut deps = Vec::new();
        while let Some(row) = rows.next().await? {
            deps.push(Dependency {
                task_id: get_string(&row, 0),
                depends_on: get_string(&row, 1),
                dep_type: get_string(&row, 2),
            });
        }
        Ok(deps)
    }

    pub async fn get_events(&self, task_id: &str) -> Result<Vec<Event>> {
        let mut stmt = self.conn.prepare("SELECT id, task_id, actor, action, field, old_value, new_value, timestamp FROM events WHERE task_id = ?1 ORDER BY id ASC").await?;
        let mut rows = stmt.query([task_id]).await?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(row_to_event(&row));
        }
        Ok(events)
    }
}

async fn create_tasks_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 0,
            assignee TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )", (),
    ).await?;
    Ok(())
}

async fn create_dependencies_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dependencies (
            task_id TEXT NOT NULL REFERENCES tasks(id),
            depends_on TEXT NOT NULL REFERENCES tasks(id),
            dep_type TEXT NOT NULL DEFAULT 'blocks',
            PRIMARY KEY (task_id, depends_on)
        )", (),
    ).await?;
    Ok(())
}

async fn create_events_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL REFERENCES tasks(id),
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            field TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT NOT NULL
        )", (),
    ).await?;
    Ok(())
}

async fn create_schema(conn: &Connection) -> Result<()> {
    create_tasks_table(conn).await?;
    create_dependencies_table(conn).await?;
    create_events_table(conn).await?;
    Ok(())
}

async fn record_event(conn: &Connection, task_id: &str, actor: &str, action: &str, field: Option<&str>, old: Option<&str>, new: Option<&str>) -> Result<()> {
    let now = now_iso();
    conn.execute(
        "INSERT INTO events (task_id, actor, action, field, old_value, new_value, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        [task_id, actor, action, field.unwrap_or(""), old.unwrap_or(""), new.unwrap_or(""), &now],
    ).await?;
    Ok(())
}

async fn apply_field_update(conn: &Connection, id: &str, actor: &str, now: &str, field: &str, old: &str, new: Option<&str>) -> Result<()> {
    let Some(new_val) = new else { return Ok(()) };
    if new_val == old { return Ok(()) }
    let sql = format!("UPDATE tasks SET {} = ?1, updated_at = ?2 WHERE id = ?3", field);
    conn.execute(&sql, [new_val, now, id]).await?;
    record_event(conn, id, actor, "updated", Some(field), Some(old), Some(new_val)).await?;
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

async fn collect_tasks(rows: &mut turso::Rows) -> Result<Vec<Task>> {
    let mut tasks = Vec::new();
    while let Some(row) = rows.next().await? {
        tasks.push(row_to_task(&row));
    }
    Ok(tasks)
}

fn row_to_task(row: &turso::Row) -> Task {
    Task {
        id: get_string(row, 0),
        title: get_string(row, 1),
        description: get_optional_string(row, 2),
        status: get_string(row, 3),
        priority: get_string(row, 4).parse().unwrap_or(0),
        assignee: get_optional_string(row, 5),
        created_at: get_string(row, 6),
        updated_at: get_string(row, 7),
    }
}

fn row_to_event(row: &turso::Row) -> Event {
    Event {
        id: get_string(row, 0).parse().unwrap_or(0),
        task_id: get_string(row, 1),
        actor: get_string(row, 2),
        action: get_string(row, 3),
        field: get_optional_string(row, 4),
        old_value: get_optional_string(row, 5),
        new_value: get_optional_string(row, 6),
        timestamp: get_string(row, 7),
    }
}

fn get_string(row: &turso::Row, idx: usize) -> String {
    match row.get_value(idx) {
        Ok(turso::Value::Text(s)) => s,
        Ok(turso::Value::Integer(n)) => n.to_string(),
        _ => String::new(),
    }
}

fn get_optional_string(row: &turso::Row, idx: usize) -> Option<String> {
    match row.get_value(idx) {
        Ok(turso::Value::Text(s)) if !s.is_empty() => Some(s),
        _ => None,
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
