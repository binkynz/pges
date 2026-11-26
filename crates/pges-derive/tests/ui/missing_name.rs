use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(version = 1)]
    Created { title: String },
    Assigned { assignee: String },
}

fn main() {}
