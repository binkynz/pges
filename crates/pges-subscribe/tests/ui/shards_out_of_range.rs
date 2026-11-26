fn main() {
    let none = pges_subscribe::shards!(0);
    let too_many = pges_subscribe::shards!(1025);
    let _ = (none, too_many);
}
