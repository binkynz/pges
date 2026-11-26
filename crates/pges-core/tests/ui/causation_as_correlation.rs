use pges_core::{commit, metadata};

fn main() {
    let cause = commit::CommitId::generate();
    let mut meta = metadata::Metadata::default();
    meta.correlation_id = Some(metadata::CausationId::from(cause));
}
