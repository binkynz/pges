use pges_sequencer::{source, source_kind};

const UPPER_CASE: source::SourceKind = source_kind!("Replaying");

fn main() {
    let _ = UPPER_CASE;
}
