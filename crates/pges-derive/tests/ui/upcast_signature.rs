use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
enum JobEvent {
    #[event(name = "job.created", version = 2, upcast(1 => up))]
    Created { title: String },
}

fn up(value: String) -> String {
    value
}

fn main() {}
