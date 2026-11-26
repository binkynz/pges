use pges_core::interval;

fn main() {
    let too_long = interval!(31 d);
    let _ = too_long;
}
