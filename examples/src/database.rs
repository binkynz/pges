//! A scratch database for one run of an example.

use anyhow::{Context, Result};
use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use url::Url;
use uuid::Uuid;

/// Names a server the examples create their database on, when `PGES_TEST_DATABASE_URL` is not set.
pub const ENV_DATABASE_URL: &str = "DATABASE_URL";

/// A database created for one run, dropped again by [`Database::cleanup`].
///
/// Where it is created, in order:
///
/// 1. `PGES_TEST_DATABASE_URL`: on that server, cloned from the test template, as the tests do.
/// 2. `DATABASE_URL`: on that server, as `pges_example_<id>`. The URL's role needs `CREATEDB`.
/// 3. Otherwise on the PostgreSQL 18 that `pges-testing` finds or starts: the local cluster from `pg-up`,
///    or a Docker container.
///
/// The examples with the logical source also need `wal_level = logical` and a role with `REPLICATION`. The
/// servers from `pges-testing` have both.
pub struct Database {
    url: String,
    owner: Owner,
}

impl Database {
    /// Creates the database.
    pub async fn create() -> Result<Self> {
        let server_url = match std::env::var_os(pges_testing::ENV_URL) {
            Some(_) => None,
            None => std::env::var(ENV_DATABASE_URL).ok(),
        };
        if let Some(server_url) = server_url {
            return Self::create_on(&server_url).await;
        }

        let db = pges_testing::TestDb::new().await?;
        Ok(Self {
            url: db.url().to_owned(),
            owner: Owner::Testing(db),
        })
    }

    /// The database's connection URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// A new pool of up to 8 connections to the database.
    pub async fn pool(&self) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(8)
            .connect(&self.url)
            .await
            .context("connecting to the example database")
    }

    /// Drops the replication slots of the database, ending the processes that still stream from them, then
    /// drops the database. A database from `pges-testing` is also dropped when the example fails before
    /// this runs.
    pub async fn cleanup(self) -> Result<()> {
        match self.owner {
            Owner::Testing(db) => db.cleanup_with_slots().await,
            Owner::Server { admin_url, name } => {
                pges_testing::drop_database(&admin_url, &name).await
            }
        }
    }

    async fn create_on(server_url: &str) -> Result<Self> {
        let name = format!("pges_example_{}", Uuid::now_v7().simple());
        let mut url = Url::parse(server_url).context("parsing DATABASE_URL")?;
        url.set_path(&format!("/{name}"));

        let mut admin = PgConnection::connect(server_url)
            .await
            .context("connecting to DATABASE_URL")?;
        admin
            .execute(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
            .await
            .with_context(|| format!("creating {name}; the role needs CREATEDB"))?;

        Ok(Self {
            url: url.into(),
            owner: Owner::Server {
                admin_url: server_url.to_owned(),
                name,
            },
        })
    }
}

/// Who created the database, and so who drops it.
enum Owner {
    Testing(pges_testing::TestDb),
    Server { admin_url: String, name: String },
}
