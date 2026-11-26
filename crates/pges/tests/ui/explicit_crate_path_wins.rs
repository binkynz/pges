// An explicit `crate = "..."` overrides the path found from Cargo.toml. This crate cannot name
// `::pges_core`, so naming it fails.

#[derive(pges::Event)]
#[event(category = "jobs", crate = "::pges_core")]
enum JobEvent {
    #[event(name = "job.created")]
    Created(String),
}

fn main() {}
