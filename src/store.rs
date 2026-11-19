//! The operations the CLI needs, implemented locally by `Database` and over
//! HTTP by `remote::HttpStore`. The HTTP API (`docs/specs/http-api.md`) maps
//! one route to each method.

use std::future::Future;

use anyhow::Result;

use crate::attachments::{Attachment, NewAttachment};
use crate::db::{Comment, Database, Dependency, Event, Task, TaskUpdates};
use crate::questions::{Answers, Question, QuestionSet};

pub trait TaskStore {
    fn create_task(
        &self,
        project: &str,
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
        project: Option<&str>,
    ) -> impl Future<Output = Result<Vec<Task>>> + Send;
    fn ready_tasks(&self) -> impl Future<Output = Result<Vec<Task>>> + Send;
    fn claim_task(
        &self,
        id: &str,
        project: &str,
        actor: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Claim the best ready task in `project`; `None` when none is ready.
    fn claim_next(
        &self,
        project: &str,
        actor: &str,
    ) -> impl Future<Output = Result<Option<Task>>> + Send;
    fn update_task(
        &self,
        id: &str,
        updates: TaskUpdates<'_>,
        actor: &str,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Open tasks whose due time has passed and is not yet delivered (reminders).
    fn due_tasks(&self) -> impl Future<Output = Result<Vec<Task>>> + Send;
    fn mark_delivered(
        &self,
        id: &str,
        due_at: &str,
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
    fn add_attachment(
        &self,
        task_id: &str,
        file: NewAttachment<'_>,
        actor: &str,
    ) -> impl Future<Output = Result<Attachment>> + Send;
    fn list_attachments(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Vec<Attachment>>> + Send;
    fn get_attachment(&self, id: i64)
    -> impl Future<Output = Result<(Attachment, Vec<u8>)>> + Send;
    fn ask_questions(
        &self,
        task_id: &str,
        questions: Vec<Question>,
        actor: &str,
    ) -> impl Future<Output = Result<QuestionSet>> + Send;
    fn list_questions(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Vec<QuestionSet>>> + Send;
    fn answer_questions(
        &self,
        id: i64,
        answers: Answers,
        actor: &str,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl TaskStore for Database {
    async fn create_task(
        &self,
        project: &str,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        Database::create_task(self, project, title, desc, priority, actor).await
    }

    async fn get_task(&self, id: &str) -> Result<Task> {
        Database::get_task(self, id).await
    }

    async fn list_tasks(
        &self,
        status: Option<&str>,
        assignee: Option<&str>,
        project: Option<&str>,
    ) -> Result<Vec<Task>> {
        Database::list_tasks(self, status, assignee, project).await
    }

    async fn ready_tasks(&self) -> Result<Vec<Task>> {
        Database::ready_tasks(self).await
    }

    async fn claim_task(&self, id: &str, project: &str, actor: &str) -> Result<()> {
        Database::claim_task(self, id, project, actor).await
    }

    async fn claim_next(&self, project: &str, actor: &str) -> Result<Option<Task>> {
        Database::claim_next(self, project, actor).await
    }

    async fn update_task(&self, id: &str, updates: TaskUpdates<'_>, actor: &str) -> Result<()> {
        Database::update_task(self, id, updates, actor).await
    }

    async fn due_tasks(&self) -> Result<Vec<Task>> {
        Database::due_tasks(self, &crate::db::now_iso()).await
    }

    async fn mark_delivered(&self, id: &str, due_at: &str, actor: &str) -> Result<()> {
        Database::mark_delivered(self, id, due_at, actor).await
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

    async fn add_attachment(
        &self,
        task_id: &str,
        file: NewAttachment<'_>,
        actor: &str,
    ) -> Result<Attachment> {
        Database::add_attachment(self, task_id, file, actor).await
    }

    async fn list_attachments(&self, task_id: &str) -> Result<Vec<Attachment>> {
        Database::list_attachments(self, task_id).await
    }

    async fn get_attachment(&self, id: i64) -> Result<(Attachment, Vec<u8>)> {
        Database::get_attachment(self, id).await
    }

    async fn ask_questions(
        &self,
        task_id: &str,
        questions: Vec<Question>,
        actor: &str,
    ) -> Result<QuestionSet> {
        Database::ask_questions(self, task_id, questions, actor).await
    }

    async fn list_questions(&self, task_id: &str) -> Result<Vec<QuestionSet>> {
        Database::list_questions(self, task_id).await
    }

    async fn answer_questions(&self, id: i64, answers: Answers, actor: &str) -> Result<()> {
        Database::answer_questions(self, id, answers, actor).await
    }
}
