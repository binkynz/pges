fn main() {
    let none = pges_subscribe::lanes!(0);
    let too_many = pges_subscribe::lanes!(32768);
    let _ = (none, too_many);
}
