use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "$stream-deleted")]
    Deleted,
    #[event(name = "")]
    Empty,
}

fn main() {}
