//! The scenarios of `pges-core/tests/event.rs`, with `#[derive(Event)]` in place of the hand-written impl.
//! `hand_written` keeps a copy of that impl, and the `same_as_hand_written_*` tests compare the two on the
//! same inputs.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pges_core::{aggregate, event, frame, metadata};
use pges_derive::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Created {
    order_id: Uuid,
    title: String,
    priority: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Assigned {
    assignee: Uuid,
}

/// Newtype variants, the shape of the hand-written impl.
#[derive(Debug, Clone, PartialEq, Event)]
#[event(category = "jobs", codec = "json")]
enum JobEvent {
    #[event(name = "job.created", version = 2, upcast(1 => upcast::created_v1))]
    Created(Created),
    #[event(name = "job.assigned")]
    Assigned(Assigned),
}

/// The same events as struct variants: the stored data is identical.
#[derive(Debug, Clone, PartialEq, Event)]
#[event(category = "jobs", codec = "json")]
enum JobEventFields {
    #[event(name = "job.created", version = 2, upcast(1 => upcast::created_v1))]
    Created {
        order_id: Uuid,
        title: String,
        priority: u8,
    },
    #[event(name = "job.assigned")]
    Assigned { assignee: Uuid },
}

mod upcast {
    use pges_core::event;

    /// Version 1 of `job.created` had no priority.
    pub fn created_v1(mut value: serde_json::Value) -> Result<serde_json::Value, event::BoxError> {
        let object = value
            .as_object_mut()
            .ok_or("job.created v1 is not an object")?;
        object.insert("priority".into(), 0.into());

        Ok(value)
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

fn v(version: u16) -> event::SchemaVersion {
    event::SchemaVersion::new(version).unwrap()
}

fn decode<E: event::Event>(type_name: &str, version: u16, data: &[u8]) -> event::Decoded<E> {
    E::decode(type_name, v(version), data).unwrap()
}

fn encode<E: event::Event>(event: &E) -> Vec<u8> {
    let mut out = Vec::new();
    event.encode(&mut out).unwrap();
    out
}

fn created() -> Created {
    Created {
        order_id: Uuid::now_v7(),
        title: "Brakes".into(),
        priority: 3,
    }
}

#[test]
fn events_round_trip_through_a_frame() {
    use event::Event;

    let events = vec![
        JobEvent::Created(created()),
        JobEvent::Assigned(Assigned {
            assignee: Uuid::now_v7(),
        }),
    ];
    let type_ids = [frame::EventTypeId::new(1), frame::EventTypeId::new(2)];

    let data: Vec<Vec<u8>> = events.iter().map(encode).collect();
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

    assert_eq!(
        decode("job.created", 1, v1.as_bytes()),
        event::Decoded::Event(JobEvent::Created(Created {
            order_id,
            title: "Paint".into(),
            priority: 0
        }))
    );
    assert_eq!(
        decode("job.created", 1, v1.as_bytes()),
        event::Decoded::Event(JobEventFields::Created {
            order_id,
            title: "Paint".into(),
            priority: 0
        })
    );
}

#[test]
fn newer_types_and_versions_are_unknown_not_errors() {
    assert_eq!(
        decode::<JobEvent>("job.closed", 1, b"{}"),
        event::Decoded::Unknown {
            type_name: "job.closed".into(),
            schema_version: v(1)
        }
    );
    assert_eq!(
        decode::<JobEvent>("job.created", 3, b"{}"),
        event::Decoded::Unknown {
            type_name: "job.created".into(),
            schema_version: v(3)
        }
    );
    assert_eq!(
        decode::<JobEventFields>("job.assigned", 2, b"{}"),
        event::Decoded::Unknown {
            type_name: "job.assigned".into(),
            schema_version: v(2)
        }
    );
}

#[test]
fn malformed_data_is_an_error() {
    for result in [
        <JobEvent as event::Event>::decode("job.assigned", v(1), b"not json").map(|_| ()),
        <JobEventFields as event::Event>::decode("job.assigned", v(1), b"not json").map(|_| ()),
    ] {
        let error = result.unwrap_err();

        assert!(
            matches!(
                error,
                event::CodecError::Decode { schema_version, .. } if schema_version == v(1)
            ),
            "{error}"
        );
    }
}

#[test]
fn a_failing_upcaster_is_an_error() {
    let error = <JobEvent as event::Event>::decode("job.created", v(1), b"[]").unwrap_err();

    assert!(
        matches!(
            &error,
            event::CodecError::Decode { type_name, schema_version, .. }
                if type_name == "job.created" && *schema_version == v(1)
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
            title: "Glass".into(),
            ..created()
        }),
        JobEvent::Assigned(Assigned { assignee }),
    ] {
        job.apply(&event);
    }

    assert_eq!(job.title.as_deref(), Some("Glass"));
    assert_eq!(job.assignee, Some(assignee));
}

#[test]
fn same_as_hand_written_constants() {
    use event::Event;

    assert_eq!(JobEvent::CATEGORY, hand_written::JobEvent::CATEGORY);
    assert_eq!(JobEvent::TYPES, hand_written::JobEvent::TYPES);
    assert_eq!(JobEventFields::CATEGORY, hand_written::JobEvent::CATEGORY);
    assert_eq!(JobEventFields::TYPES, hand_written::JobEvent::TYPES);
}

#[test]
fn same_as_hand_written_encoding() {
    use event::Event;

    let created = created();
    let assignee = Uuid::now_v7();
    let cases = [
        (
            JobEvent::Created(created.clone()),
            JobEventFields::Created {
                order_id: created.order_id,
                title: created.title.clone(),
                priority: created.priority,
            },
            hand_written::JobEvent::Created(created.clone()),
        ),
        (
            JobEvent::Assigned(Assigned { assignee }),
            JobEventFields::Assigned { assignee },
            hand_written::JobEvent::Assigned(Assigned { assignee }),
        ),
    ];

    for (newtype, fields, reference) in cases {
        assert_eq!(newtype.type_name(), reference.type_name());
        assert_eq!(fields.type_name(), reference.type_name());
        assert_eq!(newtype.schema_version(), reference.schema_version());
        assert_eq!(fields.schema_version(), reference.schema_version());
        assert_eq!(encode(&newtype), encode(&reference));
        assert_eq!(encode(&fields), encode(&reference));
    }
}

#[test]
fn same_as_hand_written_decoding() {
    let order_id = Uuid::now_v7();
    let current =
        serde_json::json!({ "order_id": order_id, "title": "Paint", "priority": 2 }).to_string();
    let v1 = serde_json::json!({ "order_id": order_id, "title": "Paint" }).to_string();
    let assigned = serde_json::json!({ "assignee": order_id }).to_string();
    let inputs: [(&str, u16, &[u8]); 8] = [
        ("job.created", 2, current.as_bytes()),
        ("job.created", 1, v1.as_bytes()),
        ("job.created", 3, current.as_bytes()),
        ("job.created", 2, b"not json"),
        ("job.assigned", 1, assigned.as_bytes()),
        ("job.assigned", 1, b"{}"),
        ("job.closed", 1, b"{}"),
        ("$stream-deleted", 1, b"{}"),
    ];

    for (type_name, version, data) in inputs {
        let version = v(version);
        let reference = outcome(<hand_written::JobEvent as event::Event>::decode(
            type_name, version, data,
        ));
        let newtype = outcome(<JobEvent as event::Event>::decode(type_name, version, data));
        let fields = outcome(<JobEventFields as event::Event>::decode(
            type_name, version, data,
        ));

        assert_eq!(newtype, reference, "{type_name} v{version}");
        assert_eq!(fields, reference, "{type_name} v{version}");
    }
}

/// What a decode produced, comparable across the three enums: the re-encoded event, the unknown type, or
/// the error's type name and version.
#[derive(Debug, PartialEq)]
enum Outcome {
    Event(&'static str, u16, Vec<u8>),
    Unknown(String, u16),
    Error(String, u16),
}

fn outcome<E: event::Event>(result: Result<event::Decoded<E>, event::CodecError>) -> Outcome {
    match result {
        Ok(event::Decoded::Event(event)) => Outcome::Event(
            event.type_name(),
            event.schema_version().get(),
            encode(&event),
        ),
        Ok(event::Decoded::Unknown {
            type_name,
            schema_version,
        }) => Outcome::Unknown(type_name, schema_version.get()),
        Ok(_) => panic!("unexpected decoding outcome"),
        Err(event::CodecError::Decode {
            type_name,
            schema_version,
            ..
        }) => Outcome::Error(type_name, schema_version.get()),
        Err(other) => panic!("unexpected error {other}"),
    }
}

/// A copy of the hand-written impl in `pges-core/tests/event.rs`.
mod hand_written {
    use pges_core::{category, codec, event};
    use serde::Deserialize;
    use uuid::Uuid;

    use super::{Assigned, Created};

    #[derive(Deserialize)]
    struct CreatedV1 {
        order_id: Uuid,
        title: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum JobEvent {
        Created(Created),
        Assigned(Assigned),
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
                Self::Created(_) => event::SchemaVersion::new(2).unwrap(),
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
}

/// Upcasters whose names collide with the parameters and locals of the generated `decode`.
mod collide {
    use pges_core::event;

    pub fn data(mut value: serde_json::Value) -> Result<serde_json::Value, event::BoxError> {
        push_step(&mut value, "data")
    }

    pub fn upcasters(mut value: serde_json::Value) -> Result<serde_json::Value, event::BoxError> {
        push_step(&mut value, "upcasters")
    }

    fn push_step(
        value: &mut serde_json::Value,
        step: &str,
    ) -> Result<serde_json::Value, event::BoxError> {
        let steps = value
            .get_mut("steps")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or("no steps")?;
        steps.push(step.into());

        Ok(value.take())
    }
}

/// Single-segment upcaster paths, resolved in the scope of the generated code.
use collide::{data, upcasters};

#[derive(Debug, PartialEq, Event)]
#[event(category = "chains")]
enum ChainEvent {
    #[event(name = "chain.stepped", version = 3, upcast(1 => data, 2 => upcasters))]
    Stepped { steps: Vec<String> },
    #[event(name = "chain.closed")]
    Closed,
}

#[test]
fn upcasters_run_from_the_stored_version_in_order() {
    let stepped = |steps: &[&str]| {
        event::Decoded::Event(ChainEvent::Stepped {
            steps: steps.iter().map(|step| (*step).to_owned()).collect(),
        })
    };

    assert_eq!(
        decode::<ChainEvent>("chain.stepped", 1, br#"{"steps":[]}"#),
        stepped(&["data", "upcasters"])
    );
    assert_eq!(
        decode::<ChainEvent>("chain.stepped", 2, br#"{"steps":[]}"#),
        stepped(&["upcasters"])
    );
    assert_eq!(
        decode::<ChainEvent>("chain.stepped", 3, br#"{"steps":[]}"#),
        stepped(&[])
    );
}

#[test]
fn unit_variants_round_trip_as_an_empty_object() {
    use event::Event;

    let closed = ChainEvent::Closed;

    assert_eq!(encode(&closed), b"{}");
    assert_eq!(
        decode::<ChainEvent>("chain.closed", 1, b"{}"),
        event::Decoded::Event(ChainEvent::Closed)
    );
    assert_eq!(closed.type_name(), "chain.closed");
}

/// `Self` in a field's type is the event enum, as anywhere else in the enum's definition. The children
/// are stored by the enum's own serde derive.
#[derive(Debug, Clone, PartialEq, Event, Serialize, Deserialize)]
#[event(category = "tree")]
enum TreeEvent {
    #[event(name = "tree.grown")]
    Grown { children: Vec<Self> },
    #[event(name = "tree.leaf")]
    Leaf,
}

#[test]
fn self_in_a_field_type_names_the_enum() {
    let grown = TreeEvent::Grown {
        children: vec![
            TreeEvent::Leaf,
            TreeEvent::Grown {
                children: Vec::new(),
            },
        ],
    };

    let data = encode(&grown);

    assert_eq!(data, br#"{"children":["Leaf",{"Grown":{"children":[]}}]}"#);
    assert_eq!(
        decode::<TreeEvent>("tree.grown", 1, &data),
        event::Decoded::Event(grown)
    );
}

/// Every serde field attribute the derive accepts, each written and read back.
#[derive(Debug, Clone, PartialEq, Event)]
#[event(category = "jobs")]
enum SerdeEvent {
    #[event(name = "job.noted")]
    Noted {
        #[serde(rename = "text", alias = "body")]
        note: String,
        #[serde(rename(serialize = "by", deserialize = "by"))]
        author: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag: Option<String>,
        #[serde(skip_serializing, default)]
        cached: u8,
        #[serde(skip)]
        local: u8,
        #[serde(with = "as_string")]
        count: u32,
        #[serde(flatten)]
        extra: Extra,
        r#type: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Extra {
    source: String,
}

mod as_string {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(value: &u32, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[test]
fn accepted_serde_field_attributes_round_trip() {
    let noted = SerdeEvent::Noted {
        note: "check pads".into(),
        author: "ana".into(),
        tag: None,
        cached: 7,
        local: 9,
        count: 12,
        extra: Extra {
            source: "web".into(),
        },
        r#type: 1,
    };

    let data = encode(&noted);

    assert_eq!(
        String::from_utf8(data.clone()).unwrap(),
        r#"{"text":"check pads","by":"ana","count":"12","source":"web","type":1}"#
    );
    assert_eq!(
        decode::<SerdeEvent>("job.noted", 1, &data),
        event::Decoded::Event(SerdeEvent::Noted {
            note: "check pads".into(),
            author: "ana".into(),
            tag: None,
            cached: 0,
            local: 0,
            count: 12,
            extra: Extra {
                source: "web".into(),
            },
            r#type: 1,
        })
    );

    let aliased = br#"{"body":"x","by":"b","tag":"t","count":"1","type":0,"source":"s"}"#;
    let event::Decoded::Event(SerdeEvent::Noted { note, tag, .. }) =
        decode::<SerdeEvent>("job.noted", 1, aliased)
    else {
        panic!("job.noted did not decode");
    };
    assert_eq!((note.as_str(), tag.as_deref()), ("x", Some("t")));
}

#[derive(Debug, PartialEq, Event)]
#[event(category = "ledger")]
enum WideEvent {
    #[event(name = "ledger.moved")]
    Moved { amount: u128, delta: i128 },
}

/// Data at the current version is read straight from the bytes: a detour through `serde_json::Value`
/// turns integers beyond 64 bits into floats, and the event the same code wrote no longer decodes.
#[test]
fn integers_beyond_64_bits_round_trip() {
    let moved = WideEvent::Moved {
        amount: u128::MAX,
        delta: i128::MIN,
    };

    let data = encode(&moved);

    assert_eq!(
        decode::<WideEvent>("ledger.moved", 1, &data),
        event::Decoded::Event(moved)
    );
}
