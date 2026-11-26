//! The `pges-bench` binary. See the `pges_bench` crate docs.

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = pges_bench::Cli::parse();
    pges_bench::run(cli).await?;
    Ok(())
}
