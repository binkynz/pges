use pges_core::category;

fn generic<T: Default>() -> (T, category::CategoryName) {
    (T::default(), category!("Jobs"))
}

fn main() {
    let _ = generic::<u8>();
}
