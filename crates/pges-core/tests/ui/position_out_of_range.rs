// A position is at most i64::MAX, the largest `int8`.

fn main() {
    let _far = pges_core::position!(u64::MAX);
}
