use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs", crate = "not a path")]
enum NotAPath {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs", crate = "pges_core::<u8>")]
enum Generic {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs", crate = "::pges_core", crate = "::pges_core")]
enum Duplicate {
    #[event(name = "job.created")]
    Created,
}

fn main() {}
