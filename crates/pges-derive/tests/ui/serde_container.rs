use pges_derive::Event;

#[derive(Event)]
#[event(category = "jobs")]
#[serde(rename_all = "camelCase")]
enum OnEnum {
    #[event(name = "job.created")]
    Created { order_id: u64 },
}

#[derive(Event)]
#[event(category = "jobs")]
enum OnVariant {
    #[event(name = "job.created")]
    #[serde(rename_all = "camelCase")]
    Created { order_id: u64 },
}

#[derive(Event)]
#[event(category = "jobs")]
enum OnNewtypeField {
    #[event(name = "job.created")]
    Created(#[serde(default)] u64),
}

fn main() {}
