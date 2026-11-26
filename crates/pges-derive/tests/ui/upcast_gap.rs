use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created", version = 4, upcast(1 => up, 3 => up))]
    Created { title: String },
    #[event(name = "job.assigned", version = 2)]
    Assigned { assignee: String },
}

fn up(value: serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError> {
    Ok(value)
}

fn main() {}
