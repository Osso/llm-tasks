//! `TaskStore` over the HTTP API in docs/specs/http-api.md (LT_URL + LT_TOKEN).

use anyhow::{Context, Result};
use reqwest::{Method, Response, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::attachments::{Attachment, NewAttachment};
use crate::db::{Comment, Conflict, Dependency, Event, Invalid, NotFound, Task, TaskUpdates};
use crate::questions::{Answers, Question, QuestionSet};
use crate::store::TaskStore;

pub struct HttpStore {
    client: reqwest::Client,
    base_url: Url,
    token: String,
}

impl HttpStore {
    pub fn new(base_url: &str, token: &str) -> Result<Self> {
        let mut base_url = Url::parse(base_url).context("invalid LT_URL")?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.host().is_none() {
            anyhow::bail!("LT_URL must be an absolute HTTP or HTTPS URL");
        }
        if base_url.query().is_some() || base_url.fragment().is_some() {
            anyhow::bail!("LT_URL must not contain a query or fragment");
        }
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        Ok(Self {
            client: reqwest::Client::new(),
            base_url,
            token: token.to_owned(),
        })
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.base_url.join(path).context("invalid API path")
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        actor: Option<&str>,
        body: Option<impl Serialize>,
    ) -> Result<T> {
        let mut request = self
            .client
            .request(method, self.url(path)?)
            .bearer_auth(&self.token);
        if let Some(actor) = actor {
            request = request.header("X-LT-Actor", actor);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        decode_response(request.send().await?).await
    }

    async fn send_empty(
        &self,
        method: Method,
        path: &str,
        actor: Option<&str>,
        body: Option<impl Serialize>,
    ) -> Result<()> {
        self.send::<serde_json::Value>(method, path, actor, body)
            .await
            .map(|_| ())
    }
}

async fn decode_response<T: DeserializeOwned>(response: Response) -> Result<T> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        let message = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| value.get("error")?.as_str().map(str::to_owned))
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
        return Err(match status {
            StatusCode::NOT_FOUND => anyhow::Error::from(NotFound(message)),
            StatusCode::CONFLICT => anyhow::Error::from(Conflict(message)),
            StatusCode::BAD_REQUEST => anyhow::Error::from(Invalid(message)),
            _ => anyhow::anyhow!("HTTP {}: {}", status, message),
        });
    }
    if status == StatusCode::NO_CONTENT || bytes.is_empty() {
        return serde_json::from_value(serde_json::Value::Null).context("empty response body");
    }
    serde_json::from_slice(&bytes).context("invalid API response JSON")
}

fn segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(encoded, "%{byte:02X}").expect("writing to String cannot fail");
        }
    }
    encoded
}

#[derive(Serialize)]
struct CreateTask<'a> {
    project: &'a str,
    title: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    priority: u8,
}

#[derive(Serialize)]
struct TaskUpdate<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignee: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_branch: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    due_at: Option<&'a str>,
}

#[derive(Serialize)]
struct ClaimNext<'a> {
    project: &'a str,
}

#[derive(Serialize)]
struct AskQuestions {
    questions: Vec<Question>,
}

#[derive(Serialize)]
struct AnswerQuestions {
    answers: Answers,
}

#[derive(Serialize)]
struct MarkDelivered<'a> {
    due_at: &'a str,
}

#[derive(Serialize)]
struct AddDependency<'a> {
    depends_on: &'a str,
    dep_type: &'a str,
}

#[derive(Serialize)]
struct AddComment<'a> {
    content: &'a str,
}

impl TaskStore for HttpStore {
    async fn create_task(
        &self,
        project: &str,
        title: &str,
        desc: Option<&str>,
        priority: u8,
        actor: &str,
    ) -> Result<Task> {
        self.send(
            Method::POST,
            "tasks",
            Some(actor),
            Some(CreateTask {
                project,
                title,
                description: desc,
                priority,
            }),
        )
        .await
    }

    async fn get_task(&self, id: &str) -> Result<Task> {
        self.send(
            Method::GET,
            &format!("tasks/{}", segment(id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn list_tasks(
        &self,
        status: Option<&str>,
        assignee: Option<&str>,
        project: Option<&str>,
    ) -> Result<Vec<Task>> {
        let mut url = self.url("tasks")?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(status) = status {
                query.append_pair("status", status);
            }
            if let Some(assignee) = assignee {
                query.append_pair("assignee", assignee);
            }
            if let Some(project) = project {
                query.append_pair("project", project);
            }
        }
        let response = self.client.get(url).bearer_auth(&self.token).send().await?;
        decode_response(response).await
    }

    async fn ready_tasks(&self) -> Result<Vec<Task>> {
        self.send(Method::GET, "tasks/ready", None, None::<serde_json::Value>)
            .await
    }

    async fn claim_task(&self, id: &str, actor: &str) -> Result<()> {
        self.send_empty(
            Method::POST,
            &format!("tasks/{}/claim", segment(id)),
            Some(actor),
            None::<serde_json::Value>,
        )
        .await
    }

    async fn claim_next(&self, project: &str, actor: &str) -> Result<Option<Task>> {
        self.send(
            Method::POST,
            "tasks/claim",
            Some(actor),
            Some(ClaimNext { project }),
        )
        .await
    }

    async fn update_task(&self, id: &str, updates: TaskUpdates<'_>, actor: &str) -> Result<()> {
        let body = TaskUpdate {
            status: updates.status,
            priority: updates.priority,
            title: updates.title,
            description: updates.description,
            assignee: updates.assignee,
            target_branch: updates.target_branch,
            due_at: updates.due_at,
            project: updates.project,
        };
        self.send_empty(
            Method::PATCH,
            &format!("tasks/{}", segment(id)),
            Some(actor),
            Some(body),
        )
        .await
    }

    async fn due_tasks(&self) -> Result<Vec<Task>> {
        self.send(Method::GET, "tasks/due", None, None::<serde_json::Value>)
            .await
    }

    async fn mark_delivered(&self, id: &str, due_at: &str, actor: &str) -> Result<()> {
        self.send_empty(
            Method::POST,
            &format!("tasks/{}/delivered", segment(id)),
            Some(actor),
            Some(MarkDelivered { due_at }),
        )
        .await
    }

    async fn close_task(&self, id: &str, actor: &str) -> Result<()> {
        self.send_empty(
            Method::POST,
            &format!("tasks/{}/close", segment(id)),
            Some(actor),
            None::<serde_json::Value>,
        )
        .await
    }

    async fn add_dependency(&self, task_id: &str, depends_on: &str, dep_type: &str) -> Result<()> {
        self.send_empty(
            Method::POST,
            &format!("tasks/{}/dependencies", segment(task_id)),
            None,
            Some(AddDependency {
                depends_on,
                dep_type,
            }),
        )
        .await
    }

    async fn remove_dependency(&self, task_id: &str, depends_on: &str) -> Result<()> {
        self.send_empty(
            Method::DELETE,
            &format!(
                "tasks/{}/dependencies/{}",
                segment(task_id),
                segment(depends_on)
            ),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn get_dependencies(&self, task_id: &str) -> Result<Vec<Dependency>> {
        self.send(
            Method::GET,
            &format!("tasks/{}/dependencies", segment(task_id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn add_comment(&self, task_id: &str, actor: &str, content: &str) -> Result<Comment> {
        self.send(
            Method::POST,
            &format!("tasks/{}/comments", segment(task_id)),
            Some(actor),
            Some(AddComment { content }),
        )
        .await
    }

    async fn get_comments(&self, task_id: &str) -> Result<Vec<Comment>> {
        self.send(
            Method::GET,
            &format!("tasks/{}/comments", segment(task_id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn get_events(&self, task_id: &str) -> Result<Vec<Event>> {
        self.send(
            Method::GET,
            &format!("tasks/{}/events", segment(task_id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn add_attachment(
        &self,
        task_id: &str,
        file: NewAttachment<'_>,
        actor: &str,
    ) -> Result<Attachment> {
        let mut url = self.url(&format!("tasks/{}/attachments", segment(task_id)))?;
        url.query_pairs_mut().append_pair("name", file.name);
        let request = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .header("X-LT-Actor", actor)
            .header(reqwest::header::CONTENT_TYPE, file.mime_type)
            .body(file.data.to_vec());
        decode_response(request.send().await?).await
    }

    async fn list_attachments(&self, task_id: &str) -> Result<Vec<Attachment>> {
        self.send(
            Method::GET,
            &format!("tasks/{}/attachments", segment(task_id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    /// Metadata comes from the task's attachment list; the route returns only bytes.
    async fn get_attachment(&self, id: i64) -> Result<(Attachment, Vec<u8>)> {
        let url = self.url(&format!("attachments/{id}"))?;
        let response = self.client.get(url).bearer_auth(&self.token).send().await?;
        if !response.status().is_success() {
            return decode_response::<serde_json::Value>(response)
                .await
                .and_then(|_| anyhow::bail!("attachment {id}: unexpected empty error"));
        }
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let metadata = header("x-lt-attachment").context("missing X-LT-Attachment header")?;
        let attachment: Attachment = serde_json::from_str(&metadata)?;
        Ok((attachment, response.bytes().await?.to_vec()))
    }

    async fn ask_questions(
        &self,
        task_id: &str,
        questions: Vec<Question>,
        actor: &str,
    ) -> Result<QuestionSet> {
        self.send(
            Method::POST,
            &format!("tasks/{}/questions", segment(task_id)),
            Some(actor),
            Some(AskQuestions { questions }),
        )
        .await
    }

    async fn list_questions(&self, task_id: &str) -> Result<Vec<QuestionSet>> {
        self.send(
            Method::GET,
            &format!("tasks/{}/questions", segment(task_id)),
            None,
            None::<serde_json::Value>,
        )
        .await
    }

    async fn answer_questions(&self, id: i64, answers: Answers, actor: &str) -> Result<()> {
        self.send_empty(
            Method::POST,
            &format!("questions/{id}/answer"),
            Some(actor),
            Some(AnswerQuestions { answers }),
        )
        .await
    }
}
