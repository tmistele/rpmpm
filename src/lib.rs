mod server;
mod md;

use std::path::PathBuf;

use clap::Parser;

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

pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    server::run(args).await?;
    Ok(())
}
