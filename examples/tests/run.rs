//! Runs every example, so `cargo test` keeps them working. Each example file is compiled here as a module
//! and its `run` function is called; its `main` is unused.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[allow(dead_code)]
#[path = "../examples/backfill.rs"]
mod backfill;
#[allow(dead_code)]
#[path = "../examples/grpc_watch.rs"]
mod grpc_watch;
#[allow(dead_code)]
#[path = "../examples/metadata.rs"]
mod metadata;
#[allow(dead_code)]
#[path = "../examples/multi_category.rs"]
mod multi_category;
#[allow(dead_code)]
#[path = "../examples/outbox.rs"]
mod outbox;
#[allow(dead_code)]
#[path = "../examples/quickstart.rs"]
mod quickstart;
#[allow(dead_code)]
#[path = "../examples/sequencer_logical.rs"]
mod sequencer_logical;
#[allow(dead_code)]
#[path = "../examples/sharded.rs"]
mod sharded;
#[allow(dead_code)]
#[path = "../examples/subscription.rs"]
mod subscription;
#[allow(dead_code)]
#[path = "../examples/unit_of_work.rs"]
mod unit_of_work;

#[tokio::test(flavor = "multi_thread")]
async fn quickstart_runs() -> anyhow::Result<()> {
    quickstart::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn unit_of_work_runs() -> anyhow::Result<()> {
    unit_of_work::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_runs() -> anyhow::Result<()> {
    subscription::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_category_runs() -> anyhow::Result<()> {
    multi_category::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn sharded_runs() -> anyhow::Result<()> {
    sharded::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn sequencer_logical_runs() -> anyhow::Result<()> {
    sequencer_logical::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn outbox_runs() -> anyhow::Result<()> {
    outbox::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_runs() -> anyhow::Result<()> {
    backfill::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_watch_runs() -> anyhow::Result<()> {
    grpc_watch::run().await
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_runs() -> anyhow::Result<()> {
    metadata::run().await
}
