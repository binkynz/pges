//! `#[event(crate = "...")]` overrides the path `#[derive(pges::Event)]` finds on its own. This crate's
//! tests have no `pges_core` dependency of that name (Cargo.toml), so naming it fails to compile.
//! Regenerate the expected output with `TRYBUILD=overwrite cargo test -p pges --test compile_fail` and
//! review the diff.

#[cfg(feature = "derive")]
#[test]
fn an_explicit_crate_path_overrides_the_found_one() {
    if !on_pinned_toolchain() {
        return;
    }

    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

/// The compiler the `.stderr` files were written with: the stable toolchain CI pins in
/// `.github/workflows/ci.yml`. rustc words its errors differently from one release to the next, so on any other
/// compiler, the MSRV (1.94) included, the cases are skipped; bump this with the CI pin and re-bless.
#[cfg(feature = "derive")]
const TOOLCHAIN: &str = "1.97.1";

/// Whether `rustc` (the one cargo runs, `RUSTC` or the rustup toolchain) is [`TOOLCHAIN`].
#[cfg(feature = "derive")]
fn on_pinned_toolchain() -> bool {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let version = std::process::Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_default();
    let pinned = version.starts_with(&format!("rustc {TOOLCHAIN} "));
    if !pinned {
        eprintln!(
            "skipping the compile-fail cases: their expected errors are rustc {TOOLCHAIN}'s, not {}",
            version.trim()
        );
    }

    pinned
}
