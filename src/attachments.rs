//! Files attached to tasks. Bytes live in `task_attachments` inside tasks.db, so
//! the database backup covers them; one file is capped at `MAX_ATTACHMENT_BYTES`.

use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::db::{Database, Invalid, NotFound, now_iso, record_event};

pub const MAX_ATTACHMENT_BYTES: usize = 10 * 1024 * 1024;

/// Attachment metadata; the bytes are fetched separately.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Attachment {
    pub id: i64,
    pub task_id: String,
    pub name: String,
    pub mime_type: String,
    pub size: i64,
    pub actor: String,
    pub created_at: String,
}

/// A file to attach.
pub struct NewAttachment<'a> {
    pub name: &'a str,
    pub mime_type: &'a str,
    pub data: &'a [u8],
}

const ATTACHMENT_COLUMNS: &str = "id, task_id, name, mime_type, size, actor, created_at";

fn attachment_from_row(row: &rusqlite::Row) -> rusqlite::Result<Attachment> {
    Ok(Attachment {
        id: row.get(0)?,
        task_id: row.get(1)?,
        name: row.get(2)?,
        mime_type: row.get(3)?,
        size: row.get(4)?,
        actor: row.get(5)?,
        created_at: row.get(6)?,
    })
}

fn validate(file: &NewAttachment<'_>) -> Result<()> {
    if file.name.trim().is_empty() || file.name.contains('/') {
        return Err(Invalid(format!("Invalid attachment name {:?}", file.name)).into());
    }
    if file.data.len() > MAX_ATTACHMENT_BYTES {
        return Err(Invalid(format!(
            "Attachment is {} bytes; the limit is {MAX_ATTACHMENT_BYTES}",
            file.data.len()
        ))
        .into());
    }
    Ok(())
}

/// Content type for a file name, from its extension; agents may override it.
pub fn guess_mime_type(name: &str) -> &'static str {
    let extension = name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase());
    match extension.as_deref() {
        Some("txt" | "log") => "text/plain",
        Some("md") => "text/markdown",
        Some("csv") => "text/csv",
        Some("html") => "text/html",
        Some("json") => "application/json",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("zip") => "application/zip",
        _ => "application/octet-stream",
    }
}

impl Database {
    pub async fn add_attachment(
        &self,
        task_id: &str,
        file: NewAttachment<'_>,
        actor: &str,
    ) -> Result<Attachment> {
        validate(&file)?;
        self.get_task(task_id).await?;
        let (task_id, actor) = (task_id.to_string(), actor.to_string());
        let (name, mime_type) = (file.name.to_string(), file.mime_type.to_string());
        let data = file.data.to_vec();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let now = now_iso();
            c.execute(
                "INSERT INTO task_attachments (task_id, name, mime_type, size, data, actor, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![&task_id, &name, &mime_type, data.len() as i64, &data, &actor, &now],
            )?;
            let id = c.last_insert_rowid();
            record_event(&c, &task_id, &actor, "attached", Some("attachment"), None, Some(&format!("{id} {name}")))?;
            Ok(Attachment { id, task_id, name, mime_type, size: data.len() as i64, actor, created_at: now })
        })
        .await?
    }

    pub async fn list_attachments(&self, task_id: &str) -> Result<Vec<Attachment>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let sql = format!(
                "SELECT {ATTACHMENT_COLUMNS} FROM task_attachments WHERE task_id = ?1 ORDER BY id"
            );
            let mut stmt = c.prepare(&sql)?;
            let rows = stmt.query_map(params![&task_id], attachment_from_row)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await?
    }

    pub async fn get_attachment(&self, id: i64) -> Result<(Attachment, Vec<u8>)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let sql =
                format!("SELECT {ATTACHMENT_COLUMNS}, data FROM task_attachments WHERE id = ?1");
            c.query_row(&sql, params![id], |row| {
                Ok((attachment_from_row(row)?, row.get(7)?))
            })
            .optional()?
            .ok_or_else(|| NotFound(format!("Attachment not found: {id}")).into())
        })
        .await?
    }
}
