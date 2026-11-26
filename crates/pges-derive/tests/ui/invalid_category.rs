use pges_derive::Event;

#[derive(Event)]
#[event(category = "Jobs")]
enum UpperCase {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "jobs-board")]
enum Dash {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "")]
enum Empty {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
#[event(category = "a_category_name_that_is_longer_than_forty_bytes")]
enum TooLong {
    #[event(name = "job.created")]
    Created,
}

#[derive(Event)]
enum Missing {
    #[event(name = "job.created")]
    Created,
}

fn main() {}
