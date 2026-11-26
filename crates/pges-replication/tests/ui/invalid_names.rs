fn main() {
    let slot = pges_replication::slot_name!("Not-A-Slot");
    let publication = pges_replication::publication_name!("1pges");
    let _ = (slot, publication);
}
