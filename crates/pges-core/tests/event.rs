//! A hand-written `Event` impl, written the way `#[derive(Event)]` will generate it. It pins down the
//! contract: stable type names, schema versions, upcasting older versions, and `Decoded::Unknown` for types
//! and versions from a newer writer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pges_core::{aggregate, category, codec, event, frame, metadata};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Created {
    order_id: Uuid,
    title: String,
    priority: u8,
}

/// Version 1 of `job.created` had no priority.
#[derive(Deserialize)]
struct CreatedV1 {
    order_id: Uuid,
    title: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Assigned {
    assignee: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
enum JobEvent {
    Created(Created),
    Assigned(Assigned),
}

const V2: event::SchemaVersion = event::SchemaVersion::new(2).unwrap();

fn v(n: u16) -> event::SchemaVersion {
    event::SchemaVersion::new(n).unwrap()
}

impl event::Event for JobEvent {
    const CATEGORY: category::CategoryName = category!("jobs");
    const TYPES: &'static [&'static str] = &["job.created", "job.assigned"];

    fn type_name(&self) -> &'static str {
        match self {
            Self::Created(_) => "job.created",
            Self::Assigned(_) => "job.assigned",
        }
    }

    fn schema_version(&self) -> event::SchemaVersion {
        match self {
            Self::Created(_) => V2,
            Self::Assigned(_) => event::SchemaVersion::FIRST,
        }
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
        let event = match (type_name, schema_version.get()) {
            ("job.created", 2) => {
                Self::Created(codec::json::decode(type_name, schema_version, data)?)
            }
            ("job.created", 1) => {
                let v1: CreatedV1 = codec::json::decode(type_name, schema_version, data)?;
                Self::Created(Created {
                    order_id: v1.order_id,
                    title: v1.title,
                    priority: 0,
                })
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
        };

        Ok(event::Decoded::Event(event))
    }
}

#[derive(Debug, Default)]
struct Job {
    title: Option<String>,
    assignee: Option<Uuid>,
}

impl aggregate::Aggregate for Job {
    type Event = JobEvent;

    fn apply(&mut self, event: &JobEvent) {
        match event {
            JobEvent::Created(e) => self.title = Some(e.title.clone()),
            JobEvent::Assigned(e) => self.assignee = Some(e.assignee),
        }
    }
}

fn decode(type_name: &str, version: u16, data: &[u8]) -> event::Decoded<JobEvent> {
    <JobEvent as event::Event>::decode(type_name, v(version), data).unwrap()
}

#[test]
fn events_round_trip_through_a_frame() {
    use event::Event;

    let events = vec![
        JobEvent::Created(Created {
            order_id: Uuid::now_v7(),
            title: "Brakes".into(),
            priority: 3,
        }),
        JobEvent::Assigned(Assigned {
            assignee: Uuid::now_v7(),
        }),
    ];
    let type_ids = [frame::EventTypeId::new(1), frame::EventTypeId::new(2)];

    let data: Vec<Vec<u8>> = events
        .iter()
        .map(|e| {
            let mut out = Vec::new();
            e.encode(&mut out).unwrap();
            out
        })
        .collect();
    let frame_events: Vec<frame::FrameEvent<'_>> = events
        .iter()
        .zip(&data)
        .zip(type_ids)
        .map(|((e, d), id)| frame::FrameEvent::new(id, e.schema_version(), d))
        .collect();
    let payload = frame::encode(
        frame::Format::LATEST,
        &metadata::Metadata::default(),
        &frame_events,
    )
    .unwrap();

    let decoded = frame::decode(frame::Format::LATEST, &payload, 2).unwrap();
    let names = |id: frame::EventTypeId| JobEvent::TYPES[id.get() as usize - 1];
    let back: Vec<JobEvent> = decoded
        .events
        .iter()
        .map(
            |e| match decode(names(e.type_id), e.schema_version.get(), e.data) {
                event::Decoded::Event(event) => event,
                other => panic!("unexpected {other:?}"),
            },
        )
        .collect();

    assert_eq!(back, events);
}

#[test]
fn older_versions_are_upcast() {
    let order_id = Uuid::now_v7();
    let v1 = serde_json::json!({ "order_id": order_id, "title": "Paint" }).to_string();

    let decoded = decode("job.created", 1, v1.as_bytes());

    assert_eq!(
        decoded,
        event::Decoded::Event(JobEvent::Created(Created {
            order_id,
            title: "Paint".into(),
            priority: 0
        }))
    );
}

#[test]
fn newer_types_and_versions_are_unknown_not_errors() {
    assert_eq!(
        decode("job.closed", 1, b"{}"),
        event::Decoded::Unknown {
            type_name: "job.closed".into(),
            schema_version: event::SchemaVersion::FIRST
        }
    );
    assert_eq!(
        decode("job.created", 3, b"{}"),
        event::Decoded::Unknown {
            type_name: "job.created".into(),
            schema_version: v(3)
        }
    );
}

#[test]
fn malformed_data_is_an_error() {
    let error = <JobEvent as event::Event>::decode("job.assigned", v(1), b"not json").unwrap_err();

    assert!(
        matches!(
            error,
            event::CodecError::Decode { schema_version, .. } if schema_version == event::SchemaVersion::FIRST
        ),
        "{error}"
    );
}

#[test]
fn aggregates_fold_events() {
    use aggregate::Aggregate;

    let assignee = Uuid::now_v7();
    let mut job = Job::default();
    for event in [
        JobEvent::Created(Created {
            order_id: Uuid::now_v7(),
            title: "Glass".into(),
            priority: 1,
        }),
        JobEvent::Assigned(Assigned { assignee }),
    ] {
        job.apply(&event);
    }

    assert_eq!(job.title.as_deref(), Some("Glass"));
    assert_eq!(job.assignee, Some(assignee));
}

#[test]
fn a_valid_type_list_passes_the_check() {
    let () = <JobEvent as event::Event>::__CHECK;
}

#[test]
fn system_types_are_reserved() {
    assert!(event::is_system_type(event::STREAM_DELETED));
    assert!(!event::is_system_type("job.created"));
}
