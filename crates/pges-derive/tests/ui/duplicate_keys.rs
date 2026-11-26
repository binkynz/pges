use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs", category = "orders")]
enum DuplicateCategory {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs", codec = "json")]
#[event(codec = "json")]
enum DuplicateCodec {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs")]
enum DuplicateVariantKeys {
    #[event(name = "job.created", name = "job.made")]
    Name,
    #[event(name = "job.assigned", version = 1, version = 1)]
    Version,
    #[event(name = "job.closed", version = 2, upcast(1 => up), upcast(1 => up))]
    Upcast,
}

fn up(value: serde_json::Value) -> Result<serde_json::Value, pges_core::event::BoxError> {
    Ok(value)
}

fn main() {}
