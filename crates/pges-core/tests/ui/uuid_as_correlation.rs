use pges_core::metadata;

fn main() {
    let mut meta = metadata::Metadata::default();
    meta.correlation_id = Some(uuid::Uuid::now_v7());
}
