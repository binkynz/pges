use pges_core::metadata;

fn main() {
    let mut meta = metadata::Metadata::default();
    let correlation: metadata::CorrelationId = uuid::Uuid::now_v7();
    meta.correlation_id = Some(correlation);
}
