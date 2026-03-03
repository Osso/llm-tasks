use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "lt", about = "Persistent task store for LLM agents")]
pub struct Cli {
    /// Database path (default: ~/.local/share/llm-tasks/tasks.db)
    #[arg(long, global = true)]
    pub db: Option<PathBuf>,

    /// Actor identity for audit trail
    #[arg(long, global = true)]
    pub actor: Option<String>,

    /// Output as JSON
    #[arg(long, global = true, default_value_t = false)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn db_path(&self) -> PathBuf {
        if let Some(p) = &self.db {
            return p.clone();
        }
        if let Ok(p) = std::env::var("LT_DB") {
            return PathBuf::from(p);
        }
        let data_dir = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
        data_dir.join("llm-tasks").join("tasks.db")
    }

    pub fn actor(&self) -> String {
        if let Some(a) = &self.actor {
            return a.clone();
        }
        if let Ok(a) = std::env::var("LT_ACTOR") {
            return a;
        }
        "unknown".to_string()
    }
}

#[derive(Subcommand)]
pub enum Command {
    /// Initialize the database
    Init,

    /// Create a new task
    Create {
        /// Task title
        title: String,
        /// Task description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (0=none, 1=low, 2=medium, 3=high)
        #[arg(short, long, default_value_t = 0)]
        priority: u8,
    },

    /// List tasks
    List {
        /// Filter by status
        #[arg(short, long)]
        status: Option<String>,
        /// Filter by assignee
        #[arg(short, long)]
        assignee: Option<String>,
    },

    /// Show unblocked tasks ready for work
    Ready,

    /// Show task details
    Show {
        /// Task ID
        id: String,
    },

    /// Atomically claim a task
    Claim {
        /// Task ID
        id: String,
    },

    /// Update a task
    Update {
        /// Task ID
        id: String,
        /// New status
        #[arg(short, long)]
        status: Option<String>,
        /// New priority
        #[arg(short, long)]
        priority: Option<u8>,
        /// New title
        #[arg(short, long)]
        title: Option<String>,
        /// New description
        #[arg(short, long)]
        description: Option<String>,
    },

    /// Close a task
    Close {
        /// Task ID
        id: String,
    },

    /// Manage dependencies
    Dep {
        #[command(subcommand)]
        command: DepCommand,
    },

    /// Show task event history
    History {
        /// Task ID
        id: String,
    },
}

#[derive(Subcommand)]
pub enum DepCommand {
    /// Add a blocking dependency
    Add {
        /// Task that is blocked
        id: String,
        /// Task that blocks it
        blocker: String,
    },
    /// Remove a dependency
    Rm {
        /// Task that was blocked
        id: String,
        /// Task that was blocking
        blocker: String,
    },
}
