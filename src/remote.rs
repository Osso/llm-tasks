//! `TaskStore` over the HTTP API in docs/specs/http-api.md (LT_URL + LT_TOKEN).

use anyhow::Result;

use crate::db::{Comment, Dependency, Event, Task, TaskUpdates};
use crate::store::TaskStore;

pub struct HttpStore {}

impl HttpStore {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        let _ = (base_url, token);
        todo!("remote slice")
    }
}

impl TaskStore for HttpStore {
    async fn create_task(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        todo!("remote slice")
    }

    async fn get_task(&self, id: &str) -> Result<Task> {
        todo!("remote slice")
    }

    async fn list_tasks(&self, status: Option<&str>, assignee: Option<&str>) -> Result<Vec<Task>> {
        todo!("remote slice")
    }

    async fn ready_tasks(&self) -> Result<Vec<Task>> {
        todo!("remote slice")
    }

    async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        todo!("remote slice")
    }

    async fn update_task(&self, id: &str, updates: TaskUpdates<'_>, actor: &str) -> Result<()> {
        todo!("remote slice")
    }

    async fn close_task(&self, id: &str, actor: &str) -> Result<()> {
        todo!("remote slice")
    }

    async fn add_dependency(&self, task_id: &str, depends_on: &str, dep_type: &str) -> Result<()> {
        todo!("remote slice")
    }

    async fn remove_dependency(&self, task_id: &str, depends_on: &str) -> Result<()> {
        todo!("remote slice")
    }

    async fn get_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        todo!("remote slice")
    }

    async fn add_comment(&self, task_id: &str, actor: &str, content: &str) -> Result<Comment> {
        todo!("remote slice")
    }

    async fn get_comments(&self, task_id: &str) -> Result<Vec<Comment>> {
        todo!("remote slice")
    }

    async fn get_events(&self, task_id: &str) -> Result<Vec<Event>> {
        todo!("remote slice")
    }
}
