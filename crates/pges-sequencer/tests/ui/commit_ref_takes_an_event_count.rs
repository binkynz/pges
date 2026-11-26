use pges_core::{category, commit, store};
use pges_sequencer::source;

fn main() {
    let _ = source::CommitRef::new(
        store::StoreId::new(1),
        commit::CommitId::generate(),
        category::CategoryId::new(1),
        0u16,
    );
}
