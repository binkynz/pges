use pges_derive::Event;

#[derive(Event)]
enum NoEventAttribute {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(codec = "json")]
enum NoCategory {
    #[event(name = "job.created")]
    Created,
}

fn main() {}
