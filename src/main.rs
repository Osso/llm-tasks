use llm_tasks::attachments::{NewAttachment, guess_mime_type};
use llm_tasks::cli::{Cli, Command, DepCommand};
use llm_tasks::db::{Database, Dependency, Event, Task, TaskUpdates, normalize_due_at};
use llm_tasks::questions::QuestionSet;
use llm_tasks::remote::HttpStore;
use llm_tasks::server;
use llm_tasks::store::TaskStore;

struct CommandContext<'a, S> {
    db: &'a S,
    actor: &'a str,
    json: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = <Cli as clap::Parser>::parse();
    if let Command::Serve {
        addr,
        token_file,
        rules_file,
        ui_addr,
    } = &cli.command
    {
        let db = Database::open(&cli.db_path()).await?;
        return server::serve(db, *addr, token_file, rules_file.clone(), *ui_addr).await;
    }
    match std::env::var("LT_URL") {
        Ok(url) => {
            let token = std::env::var("LT_TOKEN")
                .map_err(|_| anyhow::anyhow!("LT_URL is set but LT_TOKEN is missing"))?;
            dispatch(cli, &HttpStore::new(&url, &token)?).await
        }
        Err(_) => {
            let db = Database::open(&cli.db_path()).await?;
            dispatch(cli, &db).await
        }
    }
}

async fn dispatch(cli: Cli, db: &impl TaskStore) -> anyhow::Result<()> {
    let db_path = cli.db_path();
    let actor = cli.actor();
    let context = CommandContext {
        db,
        actor: &actor,
        json: cli.json,
    };

    if is_core_command(&cli.command) {
        handle_core_command(cli.command, &db_path, &context).await?;
    } else {
        handle_task_command(cli.command, &context).await?;
    }

    Ok(())
}

fn is_core_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Init
            | Command::Create { .. }
            | Command::List { .. }
            | Command::Ready
            | Command::Due
    )
}

async fn handle_core_command(
    command: Command,
    db_path: &std::path::Path,
    context: &CommandContext<'_, impl TaskStore>,
) -> anyhow::Result<()> {
    match command {
        Command::Init => println!("Database initialized at {}", db_path.display()),
        Command::Serve { .. } => unreachable!("serve is handled before dispatch"),
        Command::Create {
            title,
            description,
            priority,
            due,
            project,
        } => {
            let new_task = NewTask {
                project: &project,
                title: &title,
                description: description.as_deref(),
                priority,
                due: due.as_deref(),
            };
            cmd_create(context, new_task).await?
        }
        Command::List {
            status,
            assignee,
            project,
        } => {
            let filters = (status.as_deref(), assignee.as_deref(), project.as_deref());
            cmd_list(context.db, filters, context.json).await?
        }
        Command::Ready => print_tasks(&context.db.ready_tasks().await?, context.json)?,
        Command::Due => print_tasks(&context.db.due_tasks().await?, context.json)?,
        _ => unreachable!("core command router received a task command"),
    }
    Ok(())
}

async fn handle_task_command(
    command: Command,
    context: &CommandContext<'_, impl TaskStore>,
) -> anyhow::Result<()> {
    match command {
        Command::Show { id } => cmd_show(context.db, &id, context.json).await?,
        Command::Claim { id, project } => cmd_claim(context, id, project).await?,
        Command::Update {
            id,
            status,
            priority,
            title,
            description,
            due,
            project,
        } => {
            let updates = TaskUpdates {
                status: status.as_deref(),
                priority,
                title: title.as_deref(),
                description: description.as_deref(),
                due_at: due.as_deref(),
                project: project.as_deref(),
                ..Default::default()
            };
            context.db.update_task(&id, updates, context.actor).await?;
            println!("Updated {}", id);
        }
        Command::Close { id } => cmd_close(context.db, &id, context.actor).await?,
        Command::Comment { id, content } => {
            cmd_comment(context.db, &id, &content, context.actor, context.json).await?
        }
        Command::Dep { command } => cmd_dep(context.db, command).await?,
        Command::History { id } => cmd_history(context.db, &id, context.json).await?,
        Command::Attach {
            id,
            path,
            name,
            mime,
        } => cmd_attach(context, &id, &path, name, mime).await?,
        Command::Attachments { id } => {
            let attachments = context.db.list_attachments(&id).await?;
            print_json_or(context.json, &attachments, |a| {
                format!("{} {} {} {} bytes", a.id, a.name, a.mime_type, a.size)
            })?
        }
        Command::Attachment { id, output } => cmd_attachment(context.db, id, output).await?,
        Command::Ask { id, questions } => {
            let questions = serde_json::from_str(&read_arg(&questions)?)?;
            let set = context
                .db
                .ask_questions(&id, questions, context.actor)
                .await?;
            print_json_or(context.json, &[set], |s| {
                format!("Asked question set {} on {}", s.id, s.task_id)
            })?
        }
        Command::Questions { id } => {
            let sets = context.db.list_questions(&id).await?;
            print_json_or(context.json, &sets, format_question_set)?
        }
        Command::Answer { id, answers } => {
            let answers = serde_json::from_str(&read_arg(&answers)?)?;
            context
                .db
                .answer_questions(id, answers, context.actor)
                .await?;
            println!("Answered question set {}", id);
        }
        _ => unreachable!("task command router received a core command"),
    }
    Ok(())
}

struct NewTask<'a> {
    project: &'a str,
    title: &'a str,
    description: Option<&'a str>,
    priority: u8,
    due: Option<&'a str>,
}

/// Create the task, then set its due time; the due time is validated first so an
/// invalid one creates nothing.
async fn cmd_create(
    context: &CommandContext<'_, impl TaskStore>,
    new_task: NewTask<'_>,
) -> anyhow::Result<()> {
    let due_at = new_task.due.map(normalize_due_at).transpose()?;
    let (db, actor) = (context.db, context.actor);
    let mut task = db
        .create_task(
            new_task.project,
            new_task.title,
            new_task.description,
            new_task.priority,
            actor,
        )
        .await?;
    if let Some(due_at) = due_at.as_deref() {
        let updates = TaskUpdates {
            due_at: Some(due_at),
            ..Default::default()
        };
        db.update_task(&task.id, updates, actor).await?;
        task = db.get_task(&task.id).await?;
    }
    if context.json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("{} {}", task.id, task.title);
    }
    Ok(())
}

/// (status, assignee, project) filters.
type ListFilters<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>);

async fn cmd_list(
    db: &impl TaskStore,
    (status, assignee, project): ListFilters<'_>,
    json: bool,
) -> anyhow::Result<()> {
    let tasks = db.list_tasks(status, assignee, project).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
    } else {
        for t in &tasks {
            let who = t
                .assignee
                .as_deref()
                .map(|a| format!("({})", a))
                .unwrap_or_default();
            println!(
                "{} [{}] {} {} {}{}",
                t.id,
                t.status,
                t.project,
                t.title,
                who,
                due_suffix(t)
            );
        }
    }
    Ok(())
}

fn print_tasks(tasks: &[Task], json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(tasks)?);
    } else {
        for t in tasks {
            println!(
                "{} [p{}] {} {}{}",
                t.id,
                t.priority,
                t.project,
                t.title,
                due_suffix(t)
            );
        }
    }
    Ok(())
}

fn due_suffix(task: &Task) -> String {
    task.due_at
        .as_deref()
        .map(|due| format!(" (due {due})"))
        .unwrap_or_default()
}

async fn cmd_show(db: &impl TaskStore, id: &str, json: bool) -> anyhow::Result<()> {
    let task = db.get_task(id).await?;
    let deps = db.get_dependencies(id).await?;
    let comments = db.get_comments(id).await?;
    let attachments = db.list_attachments(id).await?;
    let questions = db.list_questions(id).await?;
    if json {
        let mut val = serde_json::to_value(&task)?;
        val["dependencies"] = serde_json::to_value(&deps)?;
        val["comments"] = serde_json::to_value(&comments)?;
        val["attachments"] = serde_json::to_value(&attachments)?;
        val["questions"] = serde_json::to_value(&questions)?;
        println!("{}", serde_json::to_string_pretty(&val)?);
    } else {
        print_task(&task, &deps);
        for a in &attachments {
            println!("File:     {} {} ({} bytes)", a.id, a.name, a.size);
        }
        for set in &questions {
            println!("{}", format_question_set(set));
        }
        for c in &comments {
            println!("  [{} @{}] {}", c.created_at, c.actor, c.content);
        }
    }
    Ok(())
}

async fn cmd_claim(
    context: &CommandContext<'_, impl TaskStore>,
    id: Option<String>,
    project: Option<String>,
) -> anyhow::Result<()> {
    let Some(project) = project else {
        anyhow::bail!("project is required to claim a task; pass --project or set LT_PROJECT");
    };
    let Some(id) = id else {
        let Some(task) = context.db.claim_next(&project, context.actor).await? else {
            anyhow::bail!("No ready task to claim");
        };
        if context.json {
            println!("{}", serde_json::to_string_pretty(&task)?);
        } else {
            println!("Claimed {} [{}] {}", task.id, task.project, task.title);
        }
        return Ok(());
    };
    context.db.claim_task(&id, &project, context.actor).await?;
    println!("Claimed {} for {}", id, context.actor);
    Ok(())
}

async fn cmd_close(db: &impl TaskStore, id: &str, actor: &str) -> anyhow::Result<()> {
    db.close_task(id, actor).await?;
    println!("Closed {}", id);
    Ok(())
}

async fn cmd_dep(db: &impl TaskStore, command: DepCommand) -> anyhow::Result<()> {
    match command {
        DepCommand::Add { id, blocker } => {
            db.add_dependency(&id, &blocker, "blocks").await?;
            println!("{} now blocks {}", blocker, id);
        }
        DepCommand::Rm { id, blocker } => {
            db.remove_dependency(&id, &blocker).await?;
            println!("Removed dependency {} -> {}", blocker, id);
        }
    }
    Ok(())
}

async fn cmd_comment(
    db: &impl TaskStore,
    id: &str,
    content: &str,
    actor: &str,
    json: bool,
) -> anyhow::Result<()> {
    let comment = db.add_comment(id, actor, content).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&comment)?);
    } else {
        println!("Comment added to {}", id);
    }
    Ok(())
}

async fn cmd_history(db: &impl TaskStore, id: &str, json: bool) -> anyhow::Result<()> {
    let events = db.get_events(id).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&events)?);
    } else {
        for e in &events {
            println!(
                "{} {} {} {}",
                e.timestamp,
                e.actor,
                e.action,
                event_detail(e)
            );
        }
    }
    Ok(())
}

fn print_task(task: &Task, deps: &[Dependency]) {
    println!("ID:       {}", task.id);
    println!("Title:    {}", task.title);
    println!("Project:  {}", task.project);
    if let Some(desc) = &task.description {
        println!("Desc:     {}", desc);
    }
    println!("Status:   {}", task.status);
    println!("Priority: {}", task.priority);
    if let Some(a) = &task.assignee {
        println!("Assignee: {}", a);
    }
    if let Some(due) = &task.due_at {
        println!("Due:      {}", due);
    }
    if let Some(delivered) = &task.delivered_at {
        println!("Delivered: {}", delivered);
    }
    println!("Created:  {}", task.created_at);
    println!("Updated:  {}", task.updated_at);
    for d in deps {
        println!("Dep:      {} {}", d.depends_on, d.dep_type);
    }
}

fn event_detail(e: &Event) -> String {
    match (&e.field, &e.old_value, &e.new_value) {
        (Some(f), Some(old), Some(new)) => format!("{}: {} -> {}", f, old, new),
        (Some(f), None, Some(new)) => format!("{}: {}", f, new),
        _ => String::new(),
    }
}

/// The argument itself, or stdin when it is `-`.
fn read_arg(value: &str) -> anyhow::Result<String> {
    if value != "-" {
        return Ok(value.to_owned());
    }
    let mut text = String::new();
    std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)?;
    Ok(text)
}

fn print_json_or<T: serde::Serialize>(
    json: bool,
    items: &[T],
    line: impl Fn(&T) -> String,
) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(items)?);
    } else {
        items.iter().for_each(|item| println!("{}", line(item)));
    }
    Ok(())
}

fn format_question_set(set: &QuestionSet) -> String {
    let status = match &set.answered_by {
        Some(who) => format!("answered by {who}"),
        None => "unanswered".to_owned(),
    };
    let mut text = format!("#{} [{}] asked by {}", set.id, status, set.actor);
    for question in &set.questions {
        let options: Vec<&str> = question.options.iter().map(|o| o.label.as_str()).collect();
        text.push_str(&format!("\n  Q: {}", question.question));
        if !options.is_empty() {
            text.push_str(&format!(" [{}]", options.join(" | ")));
        }
        if let Some(answer) = set.answers.as_ref().and_then(|a| a.get(&question.question)) {
            text.push_str(&format!("\n  A: {answer}"));
        }
    }
    text
}

async fn cmd_attach(
    context: &CommandContext<'_, impl TaskStore>,
    id: &str,
    path: &std::path::Path,
    name: Option<String>,
    mime: Option<String>,
) -> anyhow::Result<()> {
    let data = std::fs::read(path)?;
    let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("file");
    let name = name.unwrap_or_else(|| file_name.to_owned());
    let mime_type = mime.unwrap_or_else(|| guess_mime_type(&name).to_owned());
    let file = NewAttachment {
        name: &name,
        mime_type: &mime_type,
        data: &data,
    };
    let attachment = context.db.add_attachment(id, file, context.actor).await?;
    print_json_or(context.json, &[attachment], |a| {
        format!(
            "Attached {} {} ({} bytes) to {}",
            a.id, a.name, a.size, a.task_id
        )
    })
}

async fn cmd_attachment(
    db: &impl TaskStore,
    id: i64,
    output: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    let (attachment, data) = db.get_attachment(id).await?;
    match output {
        Some(path) => {
            std::fs::write(&path, &data)?;
            eprintln!(
                "Wrote {} ({} bytes) to {}",
                attachment.name,
                data.len(),
                path.display()
            );
        }
        None => std::io::Write::write_all(&mut std::io::stdout(), &data)?,
    }
    Ok(())
}
