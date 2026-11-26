use pges_core::non_empty;

fn main() {
    let roles: non_empty::NonEmpty<&str> = non_empty![];
    let _ = roles;
}
