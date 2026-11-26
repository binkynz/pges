//! A store used through the facade only: open, register a category, append, load and read, with events
//! from `#[derive(Event)]`.

#![cfg(feature = "derive")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anyhow::Result;
use futures_util::TryStreamExt;

use pges::core::{aggregate, event::Event as _, stream, version};
use pges::postgres::{admin, append, store};

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
    #[event(name = "job.assigned")]
    Assigned { assignee: String },
}

#[derive(Debug, Default)]
struct Job {
    title: String,
    assignee: Option<String>,
}

impl aggregate::Aggregate for Job {
    type Event = JobEvent;

    fn apply(&mut self, event: &JobEvent) {
        match event {
            JobEvent::Created { title } => self.title = title.clone(),
            JobEvent::Assigned { assignee } => self.assignee = Some(assignee.clone()),
        }
    }
}

#[tokio::test]
async fn a_store_works_through_the_facade() -> Result<()> {
    let db = pges_testing::TestDb::new().await?;
    let pool = db.pool().await?;
    let mut config = store::StoreConfig::new(pges::core::store_name!("ops_es"));
    config.migrate = store::AutoMigrate::CreateOrMigrate(store::StoreDefinition::default());
    let store = store::Store::open(pool.clone(), config).await?;
    store
        .admin()
        .register_category(JobEvent::CATEGORY, admin::CategoryConfig::default())
        .await?;

    let id = stream::StreamId::new_v7();
    let created = store
        .append(
            id,
            version::ExpectedVersion::NoStream,
            &[JobEvent::Created {
                title: "Brakes".into(),
            }],
        )
        .await?;
    assert_eq!(created.outcome, append::Outcome::Appended);

    let job = store.load::<Job>(id).await?;
    assert_eq!(job.state().title, "Brakes");
    assert_eq!(job.state().assignee, None);
    store
        .append(
            id,
            job.expected(),
            &[JobEvent::Assigned {
                assignee: "ana".into(),
            }],
        )
        .await?;

    let events: Vec<JobEvent> = store
        .read_stream::<JobEvent>(id, version::Version::FIRST)
        .map_ok(|recorded| recorded.event)
        .try_collect()
        .await?;
    assert_eq!(
        events,
        [
            JobEvent::Created {
                title: "Brakes".into()
            },
            JobEvent::Assigned {
                assignee: "ana".into()
            },
        ]
    );

    pool.close().await;
    db.cleanup().await
}
