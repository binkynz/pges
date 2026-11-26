use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
    #[event(name = "job.created", version = 2, upcast(1 => up))]
    CreatedAgain { title: String },
}

fn up(value: serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError> {
    Ok(value)
}

fn main() {}
