mod server;
#[cfg(not(feature = "cmark"))]
mod md;
#[cfg(feature = "cmark")]
mod md_cmark;

use anyhow::Result;

use std::path::PathBuf;

use clap::Parser;

// TODO: mathml vs katex
// TODO: revealjs

/// pmpm: pandoc markdown preview machine, a simple markdown previewer
#[derive(Parser, Debug)]
pub struct Args {
    /// Port
    #[arg(long, short, default_value_t = 9877)]
    port: u16,

    /// Root folder of the server
    #[arg(long, short='H', env="HOME")]
    home: PathBuf,
}

pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    server::run(args).await?;
    Ok(())
}
