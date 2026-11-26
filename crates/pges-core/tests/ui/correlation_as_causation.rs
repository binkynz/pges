use pges_core::metadata;

fn main() {
    let mut meta = metadata::Metadata::default();
    meta.correlation_id = Some(metadata::CorrelationId::new_v7());
    meta.causation_id = meta.correlation_id;
}
