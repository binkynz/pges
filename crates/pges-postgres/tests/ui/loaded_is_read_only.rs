// A loaded aggregate cannot be changed, so `save_snapshot` never stores state under the wrong version.

use pges_postgres::load;

fn decide<A: Default>(loaded: &mut load::Loaded<A>) {
    loaded.state = A::default();
    loaded.version = None;
}

fn main() {}
