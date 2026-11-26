use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created {
        #[serde(skip_serializing_if = "Option::is_none")]
        tag: Option<String>,
        #[serde(skip_serializing)]
        cached: u8,
        #[serde(skip_deserializing)]
        derived: u8,
        #[serde(serialize_with = "ser")]
        written: u8,
        #[serde(deserialize_with = "de")]
        read: u8,
        #[serde(rename(serialize = "a", deserialize = "b"))]
        asymmetric: u8,
        #[serde(rename(serialize = "c"))]
        one_sided: u8,
        #[serde(borrow, default)]
        borrowed: u8,
    },
}

fn main() {}
