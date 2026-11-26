//! Shared fixtures: a store with two categories, and hand-written `Event` impls.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::{NonZeroU16, NonZeroU32};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use pges_core::{aggregate, category, codec, event, version};
use pges_postgres::{admin, store};

pub const STORE: &str = "es";
pub const STORE_NAME: pges_core::store::StoreName = pges_core::store_name!("es");

pub struct Fixture {
    pub db: pges_testing::TestDb,
    pub pool: PgPool,
    pub store: store::Store,
}

impl Fixture {
    pub async fn new() -> Result<Self> {
        Self::with_outbox(store::Outbox::Disabled).await
    }

    pub async fn with_outbox(outbox: store::Outbox) -> Result<Self> {
        Self::with_definition(pges_testing::definition(outbox)).await
    }

    pub async fn with_definition(definition: store::StoreDefinition) -> Result<Self> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        pges_testing::create_store(&mut conn, &STORE_NAME, &definition).await?;
        let pool = db.pool().await?;
        let admin = admin::Admin::new(pool.clone(), STORE_NAME);
        admin
            .register_category(JOBS, admin::CategoryConfig::default())
            .await?;
        admin
            .register_category(ORDERS, admin::CategoryConfig::default())
            .await?;
        let store = store::Store::open(pool.clone(), store::StoreConfig::new(STORE_NAME)).await?;

        Ok(Self { db, pool, store })
    }

    pub async fn finish(self) -> Result<()> {
        self.pool.close().await;
        self.db.cleanup().await
    }
}

pub fn v(n: u64) -> version::Version {
    version::Version::new(n).unwrap()
}

pub const JOBS: category::CategoryName = category!("jobs");

pub const ORDERS: category::CategoryName = category!("orders");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Created {
    pub title: String,
    pub priority: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assigned {
    pub assignee: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Closed {
    pub reason: String,
}

/// Jobs as an older reader knows them.
#[derive(Debug, Clone, PartialEq)]
pub enum JobEvent {
    Created(Created),
    Assigned(Assigned),
}

impl JobEvent {
    pub fn created(title: &str) -> Self {
        Self::Created(Created {
            title: title.into(),
            priority: 1,
        })
    }

    pub fn assigned() -> Self {
        Self::Assigned(Assigned {
            assignee: Uuid::now_v7(),
        })
    }
}

impl event::Event for JobEvent {
    const CATEGORY: category::CategoryName = JOBS;
    const TYPES: &'static [&'static str] = &["job.created", "job.assigned"];

    fn type_name(&self) -> &'static str {
        match self {
            Self::Created(_) => "job.created",
            Self::Assigned(_) => "job.assigned",
        }
    }

    fn schema_version(&self) -> event::SchemaVersion {
        event::SchemaVersion::FIRST
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), event::CodecError> {
        match self {
            Self::Created(e) => codec::json::encode("job.created", e, out),
            Self::Assigned(e) => codec::json::encode("job.assigned", e, out),
        }
    }

    fn decode(
        type_name: &str,
        schema_version: event::SchemaVersion,
        data: &[u8],
    ) -> Result<event::Decoded<Self>, event::CodecError> {
        Ok(event::Decoded::Event(
            match (type_name, schema_version.get()) {
                ("job.created", 1) => {
                    Self::Created(codec::json::decode(type_name, schema_version, data)?)
                }
                ("job.assigned", 1) => {
                    Self::Assigned(codec::json::decode(type_name, schema_version, data)?)
                }
                _ => {
                    return Ok(event::Decoded::Unknown {
                        type_name: type_name.to_owned(),
                        schema_version,
                    });
                }
            },
        ))
    }
}

/// Jobs as a newer writer knows them: one more type.
#[derive(Debug, Clone, PartialEq)]
pub enum NewerJobEvent {
    Known(JobEvent),
    Closed(Closed),
}

impl event::Event for NewerJobEvent {
    const CATEGORY: category::CategoryName = JOBS;
    const TYPES: &'static [&'static str] = &["job.created", "job.assigned", "job.closed"];

    fn type_name(&self) -> &'static str {
        match self {
            Self::Known(e) => event::Event::type_name(e),
            Self::Closed(_) => "job.closed",
        }
    }

    fn schema_version(&self) -> event::SchemaVersion {
        event::SchemaVersion::FIRST
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), event::CodecError> {
        match self {
            Self::Known(e) => event::Event::encode(e, out),
            Self::Closed(e) => codec::json::encode("job.closed", e, out),
        }
    }

    fn decode(
        type_name: &str,
        schema_version: event::SchemaVersion,
        data: &[u8],
    ) -> Result<event::Decoded<Self>, event::CodecError> {
        if (type_name, schema_version.get()) == ("job.closed", 1) {
            return Ok(event::Decoded::Event(Self::Closed(codec::json::decode(
                type_name,
                schema_version,
                data,
            )?)));
        }

        Ok(
            match <JobEvent as event::Event>::decode(type_name, schema_version, data)? {
                event::Decoded::Event(e) => event::Decoded::Event(Self::Known(e)),
                event::Decoded::Unknown {
                    type_name,
                    schema_version,
                } => event::Decoded::Unknown {
                    type_name,
                    schema_version,
                },
                _ => unreachable!(),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Placed {
    pub total: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderEvent {
    Placed(Placed),
}

impl event::Event for OrderEvent {
    const CATEGORY: category::CategoryName = ORDERS;
    const TYPES: &'static [&'static str] = &["order.placed"];

    fn type_name(&self) -> &'static str {
        "order.placed"
    }

    fn schema_version(&self) -> event::SchemaVersion {
        event::SchemaVersion::FIRST
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), event::CodecError> {
        let Self::Placed(placed) = self;
        codec::json::encode("order.placed", &placed.total, out)
    }

    fn decode(
        type_name: &str,
        schema_version: event::SchemaVersion,
        data: &[u8],
    ) -> Result<event::Decoded<Self>, event::CodecError> {
        match (type_name, schema_version.get()) {
            ("order.placed", 1) => Ok(event::Decoded::Event(Self::Placed(Placed {
                total: codec::json::decode(type_name, schema_version, data)?,
            }))),
            _ => Ok(event::Decoded::Unknown {
                type_name: type_name.to_owned(),
                schema_version,
            }),
        }
    }
}

/// A job's state; snapshots only when asked.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub titles: Vec<String>,
    pub assignments: u32,
}

impl aggregate::Aggregate for Job {
    type Event = JobEvent;

    fn apply(&mut self, event: &JobEvent) {
        match event {
            JobEvent::Created(e) => self.titles.push(e.title.clone()),
            JobEvent::Assigned(_) => self.assignments += 1,
        }
    }
}

impl aggregate::Snapshotted for Job {
    const SNAPSHOT_VERSION: NonZeroU16 = NonZeroU16::MIN;
}

/// A job's state that snapshots itself every three events.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BusyJob {
    pub events: u32,
}

impl aggregate::Aggregate for BusyJob {
    type Event = JobEvent;

    fn apply(&mut self, _: &JobEvent) {
        self.events += 1;
    }
}

impl aggregate::Snapshotted for BusyJob {
    const SNAPSHOT_VERSION: NonZeroU16 = NonZeroU16::MIN;

    fn policy() -> aggregate::SnapshotPolicy {
        aggregate::SnapshotPolicy::EveryN(NonZeroU32::new(3).unwrap())
    }
}

/// `BusyJob` after its snapshot shape changed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BusyJobV2 {
    pub events: u64,
}

impl aggregate::Aggregate for BusyJobV2 {
    type Event = JobEvent;

    fn apply(&mut self, _: &JobEvent) {
        self.events += 1;
    }
}

impl aggregate::Snapshotted for BusyJobV2 {
    const SNAPSHOT_VERSION: NonZeroU16 = NonZeroU16::new(2).unwrap();
}
