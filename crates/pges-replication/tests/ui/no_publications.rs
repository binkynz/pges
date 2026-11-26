use pges_replication::{connection, name};

fn main() {
    let none: [name::PublicationName; 0] = [];
    connection::StartOptions::new(pges_replication::slot_name!("pges_16384"), none);
}
