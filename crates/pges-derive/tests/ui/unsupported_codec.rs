use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs", codec = "msgpack")]
enum JobEvent {
    #[event(name = "job.created")]
    Created { title: String },
}

fn main() {}
