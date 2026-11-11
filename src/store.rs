//! The operations the CLI needs, implemented locally by `Database` and over
//! HTTP by `remote::HttpStore`. The HTTP API (`docs/specs/http-api.md`) maps
//! one route to each method.

use std::future::Future;

use anyhow::Result;

use crate::db::{Comment, Database, Dependency, Event, Task, TaskUpdates};

pub trait TaskStore {
    fn create_task(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> impl Future<Output = Result<Task>> + Send;
    fn get_task(&self, id: &str) -> impl Future<Output = Result<Task>> + Send;
    fn list_tasks(
        &self,
        status: Option<&str>,
        assignee: Option<&str>,
    ) -> impl Future<Output = Result<Vec<Task>>> + Send;
    fn ready_tasks(&self) -> impl Future<Output = Result<Vec<Task>>> + Send;
    fn claim_task(&self, id: &str, actor: &str) -> impl Future<Output = Result<()>> + Send;
    fn update_task(
        &self,
        id: &str,
        updates: TaskUpdates<'_>,
        actor: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    fn close_task(&self, id: &str, actor: &str) -> impl Future<Output = Result<()>> + Send;
    fn add_dependency(
        &self,
        task_id: &str,
        depends_on: &str,
        dep_type: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    fn remove_dependency(
        &self,
        task_id: &str,
        depends_on: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get_dependencies(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Vec<Dependency>>> + Send;
    fn add_comment(
        &self,
        task_id: &str,
        actor: &str,
        content: &str,
    ) -> impl Future<Output = Result<Comment>> + Send;
    fn get_comments(&self, task_id: &str) -> impl Future<Output = Result<Vec<Comment>>> + Send;
    fn get_events(&self, task_id: &str) -> impl Future<Output = Result<Vec<Event>>> + Send;
}

impl TaskStore for Database {
    async fn create_task(
        &self,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        Database::create_task(self, title, desc, priority, actor).await
    }

    async fn get_task(&self, id: &str) -> Result<Task> {
        Database::get_task(self, id).await
    }

    async fn list_tasks(&self, status: Option<&str>, assignee: Option<&str>) -> Result<Vec<Task>> {
        Database::list_tasks(self, status, assignee).await
    }

    async fn ready_tasks(&self) -> Result<Vec<Task>> {
        Database::ready_tasks(self).await
    }

    async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        Database::claim_task(self, id, actor).await
    }

    async fn update_task(&self, id: &str, updates: TaskUpdates<'_>, actor: &str) -> Result<()> {
        Database::update_task(self, id, updates, actor).await
    }

    async fn close_task(&self, id: &str, actor: &str) -> Result<()> {
        Database::close_task(self, id, actor).await
    }

    async fn add_dependency(&self, task_id: &str, depends_on: &str, dep_type: &str) -> Result<()> {
        Database::add_dependency(self, task_id, depends_on, dep_type).await
    }

    async fn remove_dependency(&self, task_id: &str, depends_on: &str) -> Result<()> {
        Database::remove_dependency(self, task_id, depends_on).await
    }

    async fn get_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        Database::get_dependencies(self, task_id).await
    }

    async fn add_comment(&self, task_id: &str, actor: &str, content: &str) -> Result<Comment> {
        Database::add_comment(self, task_id, actor, content).await
    }

    async fn get_comments(&self, task_id: &str) -> Result<Vec<Comment>> {
        Database::get_comments(self, task_id).await
    }

    async fn get_events(&self, task_id: &str) -> Result<Vec<Event>> {
        Database::get_events(self, task_id).await
    }
}
