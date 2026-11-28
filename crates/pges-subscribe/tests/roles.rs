//! The roles of schema.md (Roles) with exactly the privileges it lists: a non-superuser owner migrates and
//! administers, a writer appends, reads and deletes a stream, a sequencer sequences (outbox source, or the
//! logical source from a slot it bootstraps), and a reader runs a subscription.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::TryStreamExt as _;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions};

use pges_core::{event::Event as _, stream, version};
use pges_postgres::{admin, migrate, store};
use pges_subscribe::{batch, handler, subscription::StoreExt, subscription_name};

use common::{JobEvent, STORE_NAME, Sequencer};

/// A pool whose connections act as `role`.
async fn pool_as(db: &pges_testing::TestDb, role: &str) -> Result<PgPool> {
    let set_role = format!("SET ROLE {role}; SET synchronous_commit = on");
    Ok(PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |conn, _| {
            let set_role = set_role.clone();
            Box::pin(async move {
                conn.execute(sqlx::AssertSqlSafe(set_role)).await?;
                Ok(())
            })
        })
        .connect(db.url())
        .await?)
}

struct Noop;

impl handler::PgHandler for Noop {
    async fn handle(
        &mut self,
        _tx: &mut sqlx::PgConnection,
        _batch: &batch::Batch,
    ) -> Result<(), handler::HandlerError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_documented_roles_work_with_the_outbox_source() -> Result<()> {
    roles(store::Outbox::Enabled, Publication::Kept).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_documented_roles_work_with_the_logical_source() -> Result<()> {
    roles(store::Outbox::Disabled, Publication::Kept).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_logical_sequencer_restores_a_dropped_publication_with_the_documented_privileges()
-> Result<()> {
    roles(store::Outbox::Disabled, Publication::Dropped).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Publication {
    Kept,
    Dropped,
}

async fn roles(outbox: store::Outbox, publication: Publication) -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let mut admin_conn = db.connect().await?;
    let (owner, writer, reader, sequencer) = (
        format!("{}_owner", db.name()),
        format!("{}_writer", db.name()),
        format!("{}_reader", db.name()),
        format!("{}_sequencer", db.name()),
    );
    // The template's shared schema belongs to the superuser; the owner migrates this database itself.
    admin_conn
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP PUBLICATION pges; DROP SCHEMA pges CASCADE;
             CREATE ROLE {owner}; CREATE ROLE {writer}; CREATE ROLE {reader};
             CREATE ROLE {sequencer} LOGIN REPLICATION PASSWORD 'pw';
             GRANT CREATE ON DATABASE {database} TO {owner};",
            database = db.name()
        )))
        .await?;

    let result = scenario(
        &db,
        outbox,
        publication,
        &owner,
        &writer,
        &reader,
        &sequencer,
    )
    .await;

    admin_conn
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS es CASCADE; DROP SCHEMA IF EXISTS pges CASCADE;
             DROP PUBLICATION IF EXISTS pges;
             DROP OWNED BY {owner}, {writer}, {reader}, {sequencer};
             DROP ROLE {owner}; DROP ROLE {writer}; DROP ROLE {reader}; DROP ROLE {sequencer};"
        )))
        .await?;
    drop(admin_conn);
    db.cleanup_with_slots().await?;
    result
}

async fn scenario(
    db: &pges_testing::TestDb,
    outbox: store::Outbox,
    publication: Publication,
    owner: &str,
    writer: &str,
    reader: &str,
    sequencer: &str,
) -> Result<()> {
    // Owner: migrations, categories, maintenance.
    let owner_pool = pool_as(db, owner).await?;
    let mut config = store::StoreConfig::new(STORE_NAME);
    config.migrate = store::AutoMigrate::CreateOrMigrate(pges_testing::definition(outbox));
    let owned = store::Store::open(owner_pool.clone(), config)
        .await
        .context("owner opens and creates the store")?;

    // schema.md, Roles: the grants, as the owner gives them.
    owner_pool
        .execute(sqlx::AssertSqlSafe(grants(
            outbox, owner, writer, reader, sequencer,
        )))
        .await
        .context("granting")?;

    owned
        .admin()
        .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
        .await
        .context("owner registers a category")?;
    owned.admin().maintain().await.context("owner maintains")?;
    if publication == Publication::Dropped {
        // Dropped, as by a restore with --no-publications: the sequencer creates it again and adds the store's
        // commits, which needs CREATE on the database and the privileges of the owner of `commits`.
        let mut superuser = db.connect().await?;
        superuser
            .execute(sqlx::AssertSqlSafe(format!(
                "DROP PUBLICATION pges;
                 GRANT CREATE ON DATABASE {database} TO {sequencer};
                 GRANT {owner} TO {sequencer};",
                database = db.name()
            )))
            .await?;
    }

    // Sequencer. The logical source needs a login for its replication connection.
    let sequencer_url = db
        .url()
        .replacen("postgres:postgres@", &format!("{sequencer}:pw@"), 1);
    let sequencer_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&sequencer_url)
        .await?;
    let running = match outbox {
        store::Outbox::Enabled => Sequencer::start_on(sequencer_pool, Sequencer::config())?,
        _ => {
            let replication = pges_replication::config::Config::from_url(&sequencer_url)?;
            let mut source = pges_sequencer::logical::LogicalSource::new(
                sequencer_pool.clone(),
                replication,
                pges_sequencer::logical::LogicalConfig::default(),
            );
            source
                .bootstrap()
                .await
                .context("sequencer bootstraps its slot")?;
            Sequencer::start_source(sequencer_pool, source, Sequencer::config())
        }
    };
    running.wait_leading().await.context("sequencer leads")?;

    // Writer.
    let writer_pool = pool_as(db, writer).await?;
    let store = store::Store::open(writer_pool.clone(), store::StoreConfig::new(STORE_NAME))
        .await
        .context("writer opens the store")?;
    let id = stream::StreamId::new_v7();
    store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n: 1 }],
        )
        .await
        .context("writer appends")?;
    let events: Vec<_> = store
        .read_stream::<JobEvent>(id, version::Version::FIRST)
        .try_collect()
        .await
        .context("writer reads")?;
    assert_eq!(events.len(), 1);
    let gone = stream::StreamId::new_v7();
    store
        .append(
            gone,
            version::ExpectedVersion::NoStream,
            &[JobEvent::Noted { n: 2 }],
        )
        .await?;
    store
        .delete_stream::<JobEvent>(gone, pges_testing::version(1))
        .await
        .context("writer deletes a stream")?;

    let mut owner_conn = owner_pool.acquire().await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let (head,): (i64,) =
                sqlx::query_as("SELECT coalesce(max(last_position), 0) FROM es.log")
                    .fetch_one(&mut *owner_conn)
                    .await?;
            if head >= 2 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("sequencing")??;
    drop(owner_conn);

    // Reader: a subscription.
    let reader_pool = pool_as(db, reader).await?;
    let read_store = store::Store::open(reader_pool.clone(), store::StoreConfig::new(STORE_NAME))
        .await
        .context("reader opens the store")?;
    let running_sub = common::Running::spawn(
        read_store
            .subscription(subscription_name!("roles.v1"))
            .category::<JobEvent>()
            .run_pg(Noop),
    );
    running_sub
        .wait_position(2)
        .await
        .with_context(|| format!("reader subscribes: {:?}", running_sub.errors()))?;
    running_sub.stop().await?;
    running.stop().await?;

    if publication == Publication::Dropped {
        // The publication now belongs to the sequencer's role, so the owner cannot publish a new store.
        let mut conn = owner_pool.acquire().await?;
        let error = migrate::run_store(
            &mut conn,
            &pges_core::store_name!("es2"),
            &pges_testing::definition(outbox),
        )
        .await
        .unwrap_err();
        let migrate::MigrateError::Failed { source, .. } = &error else {
            panic!("{error}");
        };
        assert_eq!(source.code().as_deref(), Some("42501"), "{error}");
    }

    Ok(())
}

/// schema.md, Roles, as written: each role's grants, the grants for category partitions created later, and
/// the grants that depend on the source.
fn grants(
    outbox: store::Outbox,
    owner: &str,
    writer: &str,
    reader: &str,
    sequencer: &str,
) -> String {
    let roles = format!(
        "GRANT USAGE ON SCHEMA pges, es TO {writer}, {reader}, {sequencer};
         ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA es GRANT SELECT ON TABLES TO {writer}, {reader};
         GRANT SELECT ON ALL TABLES IN SCHEMA es TO {writer}, {reader};
         GRANT SELECT ON pges.stores, pges.scopes, pges.migrations TO {writer}, {reader};
         GRANT INSERT, UPDATE ON es.streams TO {writer};
         GRANT INSERT ON es.commits, es.event_types TO {writer};
         GRANT INSERT, UPDATE ON es.snapshots TO {writer};
         GRANT DELETE ON es.commits, es.snapshots TO {writer};
         GRANT INSERT ON es.deleted_commits TO {writer};
         GRANT EXECUTE ON FUNCTION pges.check_commit_id(uuid, interval) TO {writer};
         GRANT SELECT, INSERT, UPDATE ON es.subscriptions, es.backfills TO {reader};
         GRANT SELECT, INSERT, UPDATE, DELETE ON es.subscription_members TO {reader};
         GRANT SELECT ON pges.sequencer, pges.scopes, pges.stores, es.categories, es.log TO {sequencer};
         GRANT INSERT ON es.log TO {sequencer};
         GRANT UPDATE ON es.categories, pges.stores, pges.sequencer TO {sequencer};"
    );
    let source = match outbox {
        store::Outbox::Enabled => format!(
            "GRANT INSERT ON es.pending TO {writer};
             GRANT SELECT, DELETE ON es.pending TO {sequencer};"
        ),
        _ => format!("GRANT SELECT ON es.commits TO {sequencer};"),
    };

    roles + &source
}
