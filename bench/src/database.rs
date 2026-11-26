//! Scratch databases, one per measurement, and the store each benchmark writes to.

use std::str::FromStr;

use anyhow::{Context, Result};
use sqlx::{
    Connection, Executor, PgConnection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use url::Url;
use uuid::Uuid;

use pges::core::{event::Event as _, store as core_store};
use pges::postgres::{admin, sql::template, store};

use crate::event;

/// Where scratch databases are created.
#[derive(Debug, Clone)]
pub enum Server {
    /// On the server of this database URL, as `pges_bench_<id>`.
    Url(String),
    /// On the server `pges-testing` finds or starts, cloned from its migrated template.
    Testing,
}

impl Server {
    /// The server of `database_url`, or `pges-testing`'s without one.
    pub fn new(database_url: Option<String>) -> Self {
        database_url.map_or(Self::Testing, Self::Url)
    }

    /// Creates a scratch database.
    pub async fn scratch(&self) -> Result<Scratch> {
        match self {
            Self::Testing => {
                let db = pges_testing::TestDb::new().await?;
                Ok(Scratch {
                    url: db.url().to_owned(),
                    owner: Owner::Testing(db),
                })
            }
            Self::Url(server_url) => {
                let name = format!("pges_bench_{}", Uuid::now_v7().simple());
                let mut url = Url::parse(server_url).context("parsing --database-url")?;
                url.set_path(&format!("/{name}"));

                let mut admin = PgConnection::connect(server_url)
                    .await
                    .context("connecting to --database-url")?;
                let database = template::Ident::new(name.as_str())
                    .with_context(|| format!("database name {name}"))?;
                let create = pges_testing::sql::statement::CREATE_DATABASE.render(
                    &template::Values::new().ident(template::IdentPlaceholder::Database, &database),
                );
                admin
                    .execute(template::text(create.as_str()))
                    .await
                    .with_context(|| format!("creating {name}; the role needs CREATEDB"))?;

                Ok(Scratch {
                    url: url.into(),
                    owner: Owner::Server {
                        admin_url: server_url.clone(),
                        name,
                    },
                })
            }
        }
    }
}

/// A database created for one measurement. [`Scratch::cleanup`] drops it with its replication slots.
pub struct Scratch {
    url: String,
    owner: Owner,
}

impl Scratch {
    /// The database's connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new pool of up to `connections` connections to the database.
    pub async fn pool(&self, connections: u32) -> Result<PgPool> {
        self.pool_with(connections, SynchronousCommit::Server).await
    }

    /// A new pool of up to `connections` connections whose sessions use `synchronous_commit`.
    pub async fn pool_with(
        &self,
        connections: u32,
        synchronous_commit: SynchronousCommit,
    ) -> Result<PgPool> {
        let mut options =
            PgConnectOptions::from_str(&self.url).context("parsing the scratch database URL")?;
        match synchronous_commit {
            SynchronousCommit::Server => {}
            SynchronousCommit::On => options = options.options([("synchronous_commit", "on")]),
            SynchronousCommit::Off => options = options.options([("synchronous_commit", "off")]),
        }

        PgPoolOptions::new()
            .max_connections(connections)
            .connect_with(options)
            .await
            .context("connecting to the scratch database")
    }

    /// Drops the database and its replication slots, ending the processes that still use them.
    pub async fn cleanup(self) -> Result<()> {
        match self.owner {
            Owner::Testing(db) => db.cleanup_with_slots().await,
            Owner::Server { admin_url, name } => {
                pges_testing::drop_database(&admin_url, &name).await
            }
        }
    }
}

/// The `synchronous_commit` of the writers' sessions.
///
/// Logical decoding sees a commit only once its WAL is flushed. With `off`, a commit may wait for the WAL
/// writer (`wal_writer_delay`, 200 ms by default) before the logical source can see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SynchronousCommit {
    /// The server's setting.
    Server,
    /// Each commit waits for its WAL flush.
    On,
    /// Commits return before their WAL is flushed.
    Off,
}

/// Opens (and creates) the store `name` on `pool`, registers [`event::BenchEvent`]'s category and creates
/// the partitions the appends and the sequencer write to.
pub async fn open_store(
    pool: PgPool,
    name: core_store::StoreName,
    outbox: store::Outbox,
) -> Result<store::Store> {
    let mut definition = store::StoreDefinition::default();
    definition.outbox = outbox;
    let mut config = store::StoreConfig::new(name);
    config.migrate = store::AutoMigrate::CreateOrMigrate(definition);
    let store = store::Store::open(pool, config)
        .await
        .context("opening the store")?;

    let admin = store.admin();
    admin
        .register_category(
            event::BenchEvent::CATEGORY,
            admin::CategoryConfig::default(),
        )
        .await
        .context("registering the category")?;
    admin.maintain().await.context("creating partitions")?;
    store
        .register::<event::BenchEvent>()
        .await
        .context("registering the event type")?;

    Ok(store)
}

/// Who created the database, and so who drops it.
enum Owner {
    Testing(pges_testing::TestDb),
    Server { admin_url: String, name: String },
}
