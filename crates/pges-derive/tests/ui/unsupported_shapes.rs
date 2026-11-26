use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
struct NotAnEnum {
    title: String,
}

#[derive(Event)]
#[event(category = "jobs")]
enum Generic<T> {
    #[event(name = "job.created")]
    Created(T),
}

#[derive(Event)]
#[event(category = "jobs")]
enum Tuple {
    #[event(name = "job.created")]
    Created(String, u8),
}

#[derive(Event)]
#[event(category = "jobs")]
enum NoVariants {}

#[derive(Event)]
#[event(category = "jobs", owner = "me")]
enum UnknownEnumKey {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs")]
enum UnknownVariantKey {
    #[event(name = "job.created", since = 2)]
    Created,
}

#[derive(Event)]
#[event(category = "jobs")]
enum VersionZero {
    #[event(name = "job.created", version = 0)]
    Created,
}

fn main() {}
