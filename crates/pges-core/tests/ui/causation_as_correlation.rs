use pges_core::{commit, metadata};

fn main() {
    let cause = commit::CommitId::generate();
    let mut meta = metadata::Metadata::default();
    let correlation: metadata::CorrelationId = metadata::CausationId::from(cause);
    meta.correlation_id = Some(correlation);
}
