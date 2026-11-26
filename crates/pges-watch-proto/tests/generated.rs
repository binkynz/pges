//! The generated code in `src/generated/` is checked in, so building this crate needs neither `protoc` nor a
//! build script. This test generates it again from `proto/` with protox (a protobuf compiler in Rust) and
//! fails if the checked-in copy differs.
//!
//! After changing the proto, regenerate with:
//!
//! ```sh
//! PGES_REGENERATE=1 cargo test -p pges-watch-proto --test generated
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{fs, path::Path};

const GENERATED: &str = "pges.watch.v1.rs";

#[test]
fn the_checked_in_code_matches_the_proto() {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("pges-watch-proto");
    fs::create_dir_all(&out_dir).unwrap();

    let descriptors = protox::compile(["pges/watch/v1/watch.proto"], [crate_dir.join("proto")])
        .expect("the proto compiles");
    tonic_prost_build::configure()
        .build_transport(false)
        .client_mod_attribute(".", r#"#[cfg(feature = "client")]"#)
        .server_mod_attribute(".", r#"#[cfg(feature = "server")]"#)
        .out_dir(&out_dir)
        .compile_fds(descriptors)
        .expect("the code generates");

    let generated = fs::read_to_string(out_dir.join(GENERATED)).unwrap();
    let checked_in_path = crate_dir.join("src/generated").join(GENERATED);
    let checked_in = fs::read_to_string(&checked_in_path).unwrap_or_default();
    if generated == checked_in {
        return;
    }

    if std::env::var_os("PGES_REGENERATE").is_some() {
        fs::create_dir_all(checked_in_path.parent().unwrap()).unwrap();
        fs::write(&checked_in_path, generated).unwrap();
        return;
    }
    panic!(
        "{} is out of date; regenerate it with \
         `PGES_REGENERATE=1 cargo test -p pges-watch-proto --test generated`",
        checked_in_path.display()
    );
}
