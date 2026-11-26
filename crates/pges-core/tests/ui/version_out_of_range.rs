// A version is between 1 and i64::MAX.

fn main() {
    let _zero = pges_core::version!(0);
    let _far = pges_core::version!(i64::MAX.unsigned_abs() + 1);
}
