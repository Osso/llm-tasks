use llm_tasks::cli::{Cli, Command, DepCommand};
use llm_tasks::db::{Database, Dependency, Event, Task, TaskUpdates};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = <Cli as clap::Parser>::parse();
    let db = Database::open(&cli.db_path()).await?;
    dispatch(cli, &db).await
}

async fn dispatch(cli: Cli, db: &Database) -> anyhow::Result<()> {
    let actor = cli.actor();
    let json = cli.json;
    match cli.command {
        Command::Init => println!("Database initialized at {}", cli.db_path().display()),
        Command::Create { title, description, priority } => {
            cmd_create(db, &title, description.as_deref(), priority, &actor, json).await?
        }
        Command::List { status, assignee } => {
            cmd_list(db, status.as_deref(), assignee.as_deref(), json).await?
        }
        Command::Ready => cmd_ready(db, json).await?,
        Command::Show { id } => cmd_show(db, &id, json).await?,
        Command::Claim { id } => cmd_claim(db, &id, &actor).await?,
        Command::Update { id, status, priority, title, description } => {
            let s = status.as_deref();
            let t = title.as_deref();
            let d = description.as_deref();
            cmd_update(db, &id, s, priority, t, d, &actor).await?
        }
        Command::Close { id } => cmd_close(db, &id, &actor).await?,
        Command::Comment { id, content } => cmd_comment(db, &id, &content, &actor, json).await?,
        Command::Dep { command } => cmd_dep(db, command).await?,
        Command::History { id } => cmd_history(db, &id, json).await?,
    }
    Ok(())
}

async fn cmd_create(
    db: &Database,
    title: &str,
    desc: Option<&str>,
    priority: u8,
    actor: &str,
    json: bool,
) -> anyhow::Result<()> {
    let task = db.create_task(title, desc, priority, actor).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("{} {}", task.id, task.title);
    }
    Ok(())
}

async fn cmd_list(
    db: &Database,
    status: Option<&str>,
    assignee: Option<&str>,
    json: bool,
) -> anyhow::Result<()> {
    let tasks = db.list_tasks(status, assignee).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
    } else {
        for t in &tasks {
            let who = t
                .assignee
                .as_deref()
                .map(|a| format!("({})", a))
                .unwrap_or_default();
            println!("{} [{}] {} {}", t.id, t.status, t.title, who);
        }
    }
    Ok(())
}

async fn cmd_ready(db: &Database, json: bool) -> anyhow::Result<()> {
    let tasks = db.ready_tasks().await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
    } else {
        for t in &tasks {
            println!("{} [p{}] {}", t.id, t.priority, t.title);
        }
    }
    Ok(())
}

async fn cmd_show(db: &Database, id: &str, json: bool) -> anyhow::Result<()> {
    let task = db.get_task(id).await?;
    let deps = db.get_dependencies(id).await?;
    let comments = db.get_comments(id).await?;
    if json {
        let mut val = serde_json::to_value(&task)?;
        val["dependencies"] = serde_json::to_value(&deps)?;
        val["comments"] = serde_json::to_value(&comments)?;
        println!("{}", serde_json::to_string_pretty(&val)?);
    } else {
        print_task(&task, &deps);
        for c in &comments {
            println!("  [{} @{}] {}", c.created_at, c.actor, c.content);
        }
    }
    Ok(())
}

async fn cmd_claim(db: &Database, id: &str, actor: &str) -> anyhow::Result<()> {
    db.claim_task(id, actor).await?;
    println!("Claimed {} for {}", id, actor);
    Ok(())
}

async fn cmd_update(
    db: &Database,
    id: &str,
    status: Option<&str>,
    priority: Option<u8>,
    title: Option<&str>,
    desc: Option<&str>,
    actor: &str,
) -> anyhow::Result<()> {
    let updates = TaskUpdates { status, priority, title, description: desc, ..Default::default() };
    db.update_task(id, updates, actor).await?;
    println!("Updated {}", id);
    Ok(())
}

async fn cmd_close(db: &Database, id: &str, actor: &str) -> anyhow::Result<()> {
    db.close_task(id, actor).await?;
    println!("Closed {}", id);
    Ok(())
}

async fn cmd_dep(db: &Database, command: DepCommand) -> anyhow::Result<()> {
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

async fn cmd_comment(db: &Database, id: &str, content: &str, actor: &str, json: bool) -> anyhow::Result<()> {
    let comment = db.add_comment(id, actor, content).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&comment)?);
    } else {
        println!("Comment added to {}", id);
    }
    Ok(())
}

async fn cmd_history(db: &Database, id: &str, json: bool) -> anyhow::Result<()> {
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
    if let Some(desc) = &task.description {
        println!("Desc:     {}", desc);
    }
    println!("Status:   {}", task.status);
    println!("Priority: {}", task.priority);
    if let Some(a) = &task.assignee {
        println!("Assignee: {}", a);
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
