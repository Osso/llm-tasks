//! `lt serve`: the HTTP API in docs/specs/http-api.md over a local `Database`.

use std::net::SocketAddr;
use std::path::Path;

use crate::db::Database;

pub async fn serve(db: Database, addr: SocketAddr, token_file: &Path) -> anyhow::Result<()> {
    let _ = (db, addr, token_file);
    todo!("server slice")
}
