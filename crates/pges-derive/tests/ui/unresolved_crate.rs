use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs", crate = "::no_such_crate")]
enum JobEvent {
    #[event(name = "job.created")]
    Created(String),
}

fn main() {}
