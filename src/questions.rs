//! Structured questions on a task, in the shape of Pi's `ask_questions` tool. A
//! task with an unanswered question set is not ready; the orchestrator watching
//! the tracker answers it or forwards it to the Supervisor.

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use crate::db::{Conflict, Database, Invalid, NotFound, now_iso, record_event};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct QuestionOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Question {
    pub question: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// 2-4 choices; empty for a free-text answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<QuestionOption>,
    #[serde(default, rename = "multiSelect")]
    pub multi_select: bool,
}

/// Answers keyed by question text.
pub type Answers = BTreeMap<String, String>;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct QuestionSet {
    pub id: i64,
    pub task_id: String,
    pub actor: String,
    pub questions: Vec<Question>,
    pub answers: Option<Answers>,
    pub answered_by: Option<String>,
    pub created_at: String,
    pub answered_at: Option<String>,
}

const QUESTION_COLUMNS: &str =
    "id, task_id, actor, questions, answers, answered_by, created_at, answered_at";

fn invalid(message: impl Into<String>) -> anyhow::Error {
    Invalid(message.into()).into()
}

fn validate_question(question: &Question) -> Result<()> {
    if question.question.trim().is_empty() {
        return Err(invalid("Question text must not be empty"));
    }
    let labels: HashSet<&str> = question.options.iter().map(|o| o.label.trim()).collect();
    let options_valid = question.options.is_empty()
        || ((2..=4).contains(&question.options.len())
            && labels.len() == question.options.len()
            && !labels.contains(""));
    if !options_valid {
        return Err(invalid(format!(
            "{:?}: give no options or 2-4 distinct non-empty labels",
            question.question
        )));
    }
    Ok(())
}

pub fn validate_questions(questions: &[Question]) -> Result<()> {
    if !(1..=4).contains(&questions.len()) {
        return Err(invalid("Ask 1-4 questions"));
    }
    let texts: HashSet<&str> = questions.iter().map(|q| q.question.as_str()).collect();
    if texts.len() != questions.len() {
        return Err(invalid("Question texts must be distinct"));
    }
    questions.iter().try_for_each(validate_question)
}

/// Every question needs a non-empty answer and no answer may name an unknown question.
fn validate_answers(questions: &[Question], answers: &Answers) -> Result<()> {
    let expected: HashSet<&str> = questions.iter().map(|q| q.question.as_str()).collect();
    let given: HashSet<&str> = answers.keys().map(String::as_str).collect();
    if expected != given || answers.values().any(|a| a.trim().is_empty()) {
        return Err(invalid(format!(
            "Answer every question exactly, keyed by its text: {:?}",
            expected
        )));
    }
    Ok(())
}

fn question_set_from_row(row: &rusqlite::Row) -> rusqlite::Result<QuestionSet> {
    let json_column = |index: usize| -> rusqlite::Result<Option<String>> { row.get(index) };
    let parse_error = |index: usize, error: serde_json::Error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    };
    let questions: String = row.get(3)?;
    let answers = json_column(4)?
        .map(|text| serde_json::from_str(&text).map_err(|e| parse_error(4, e)))
        .transpose()?;
    Ok(QuestionSet {
        id: row.get(0)?,
        task_id: row.get(1)?,
        actor: row.get(2)?,
        questions: serde_json::from_str(&questions).map_err(|e| parse_error(3, e))?,
        answers,
        answered_by: row.get(5)?,
        created_at: row.get(6)?,
        answered_at: row.get(7)?,
    })
}

impl Database {
    pub async fn ask_questions(
        &self,
        task_id: &str,
        questions: Vec<Question>,
        actor: &str,
    ) -> Result<QuestionSet> {
        validate_questions(&questions)?;
        self.get_task(task_id).await?;
        let (task_id, actor) = (task_id.to_string(), actor.to_string());
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let now = now_iso();
            c.execute(
                "INSERT INTO task_questions (task_id, actor, questions, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![&task_id, &actor, serde_json::to_string(&questions)?, &now],
            )?;
            let id = c.last_insert_rowid();
            record_event(&c, &task_id, &actor, "asked", Some("questions"), None, Some(&id.to_string()))?;
            Ok(QuestionSet {
                id,
                task_id,
                actor,
                questions,
                answers: None,
                answered_by: None,
                created_at: now,
                answered_at: None,
            })
        })
        .await?
    }

    pub async fn list_questions(&self, task_id: &str) -> Result<Vec<QuestionSet>> {
        let conn = self.conn.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            let c = conn.lock().unwrap();
            let sql = format!(
                "SELECT {QUESTION_COLUMNS} FROM task_questions WHERE task_id = ?1 ORDER BY id"
            );
            let mut stmt = c.prepare(&sql)?;
            let rows = stmt.query_map(params![&task_id], question_set_from_row)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(Into::into)
        })
        .await?
    }

    /// Answer a question set once; a second answer is a Conflict.
    pub async fn answer_questions(&self, id: i64, answers: Answers, actor: &str) -> Result<()> {
        let actor = actor.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let c = conn.lock().unwrap();
            let sql = format!("SELECT {QUESTION_COLUMNS} FROM task_questions WHERE id = ?1");
            let set = c
                .query_row(&sql, params![id], question_set_from_row)
                .optional()?
                .ok_or_else(|| NotFound(format!("Question set not found: {id}")))?;
            validate_answers(&set.questions, &answers)?;
            let answered = c.execute(
                "UPDATE task_questions SET answers = ?1, answered_by = ?2, answered_at = ?3 WHERE id = ?4 AND answered_at IS NULL",
                params![serde_json::to_string(&answers)?, &actor, now_iso(), id],
            )?;
            if answered == 0 {
                return Err(Conflict(format!("Question set {id} is already answered")).into());
            }
            record_event(&c, &set.task_id, &actor, "answered", Some("questions"), None, Some(&id.to_string()))
        })
        .await?
    }
}
