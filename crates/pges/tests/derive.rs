//! `#[derive(pges::Event)]` through the facade, in a crate that cannot name `::pges_core` (Cargo.toml):
//! every variant shape, serde field attributes and upcasters work without `#[event(crate = "...")]`, and
//! the explicit path still works through `pges::derive::Event`.

#![cfg(feature = "derive")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde::{Deserialize, Serialize};

use pges::core::event::{self, Event as _};

mod upcast {
    use pges::core::event;
    use serde_json::Value;

    /// Version 2 renamed `name` to `title`.
    pub fn created_v1(mut value: Value) -> Result<Value, event::BoxError> {
        let object = value.as_object_mut().ok_or("not an object")?;
        let name = object.remove("name").ok_or("no name")?;
        object.insert("title".into(), name);
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Assigned {
    assignee: String,
}

#[derive(Debug, Clone, PartialEq, pges::Event)]
#[event(category = "jobs", codec = "json")]
enum JobEvent {
    #[event(name = "job.created", version = 2, upcast(1 => upcast::created_v1))]
    Created {
        title: String,
        #[serde(default)]
        priority: u8,
    },
    #[event(name = "job.assigned")]
    Assigned(Assigned),
    #[event(name = "job.closed")]
    Closed,
}

fn encode(event: &JobEvent) -> String {
    let mut data = Vec::new();
    event.encode(&mut data).unwrap();
    String::from_utf8(data).unwrap()
}

fn v(version: u16) -> event::SchemaVersion {
    event::SchemaVersion::new(version).unwrap()
}

#[test]
fn the_derive_works_through_the_facade() {
    assert_eq!(JobEvent::CATEGORY.as_str(), "jobs");
    assert_eq!(
        JobEvent::TYPES,
        &["job.created", "job.assigned", "job.closed"]
    );

    let created = JobEvent::Created {
        title: "Brakes".into(),
        priority: 0,
    };
    let assigned = JobEvent::Assigned(Assigned {
        assignee: "ana".into(),
    });
    assert_eq!(encode(&created), r#"{"title":"Brakes","priority":0}"#);
    assert_eq!(encode(&assigned), r#"{"assignee":"ana"}"#);
    assert_eq!(encode(&JobEvent::Closed), "{}");
    assert_eq!(created.schema_version(), v(2));

    assert_eq!(
        JobEvent::decode("job.created", v(1), br#"{"name":"Brakes"}"#).unwrap(),
        event::Decoded::Event(created)
    );
    assert_eq!(
        JobEvent::decode("job.assigned", v(1), br#"{"assignee":"ana"}"#).unwrap(),
        event::Decoded::Event(assigned)
    );
    assert_eq!(
        JobEvent::decode("job.closed", v(1), b"{}").unwrap(),
        event::Decoded::Event(JobEvent::Closed)
    );
    assert_eq!(
        JobEvent::decode("job.reopened", v(1), b"{}").unwrap(),
        event::Decoded::Unknown {
            type_name: "job.reopened".into(),
            schema_version: v(1)
        }
    );
}

#[test]
fn events_round_trip_through_their_encoding() {
    let events = [
        JobEvent::Created {
            title: "Brakes".into(),
            priority: 3,
        },
        JobEvent::Assigned(Assigned {
            assignee: "ana".into(),
        }),
        JobEvent::Closed,
    ];

    for event in events {
        let mut data = Vec::new();
        event.encode(&mut data).unwrap();
        let decoded = JobEvent::decode(event.type_name(), event.schema_version(), &data).unwrap();
        assert_eq!(decoded, event::Decoded::Event(event));
    }
}

/// The module path and an explicit `crate` still work.
#[derive(Debug, Clone, PartialEq, pges::derive::Event)]
#[event(crate = "pges::core", category = "invoices")]
enum InvoiceEvent {
    #[event(name = "invoice.issued")]
    Issued { total: u64 },
}

#[test]
fn an_explicit_crate_path_still_works() {
    let issued = InvoiceEvent::Issued { total: 120 };
    let mut data = Vec::new();
    issued.encode(&mut data).unwrap();

    assert_eq!(InvoiceEvent::CATEGORY.as_str(), "invoices");
    assert_eq!(
        InvoiceEvent::decode("invoice.issued", v(1), &data).unwrap(),
        event::Decoded::Event(issued)
    );
}

/// `subscription::StoreExt` is reachable at `pges::subscribe::subscription`.
#[cfg(feature = "subscribe")]
#[allow(dead_code)]
fn subscriptions_are_reachable(store: &pges::postgres::store::Store) {
    use pges::subscribe::subscription::{self, StoreExt};

    let _builder: subscription::Builder =
        store.subscription(pges::subscribe::subscription_name!("jobs-board.v1"));
}
