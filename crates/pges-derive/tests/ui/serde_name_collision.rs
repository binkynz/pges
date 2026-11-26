use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created {
        title: String,
        #[serde(rename = "title")]
        name: String,
    },
    #[event(name = "job.assigned")]
    Assigned {
        assignee: String,
        #[serde(alias = "assignee")]
        owner: String,
        r#type: u8,
        #[serde(rename = "type")]
        kind: u8,
    },
    #[event(name = "job.closed")]
    Closed {
        #[serde(skip)]
        title: String,
        #[serde(rename = "title")]
        name: String,
    },
}

fn main() {}
