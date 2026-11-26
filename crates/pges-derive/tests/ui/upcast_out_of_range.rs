use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created", version = 2, upcast(1 => up, 2 => up))]
    Created { title: String },
    #[event(name = "job.assigned", upcast(0 => up))]
    Assigned { assignee: String },
    #[event(name = "job.closed", version = 3, upcast(1 => up, 1 => up, 2 => up))]
    Closed,
}

fn up(value: serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError> {
    Ok(value)
}

fn main() {}
