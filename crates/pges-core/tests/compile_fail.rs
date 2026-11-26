//! Misuse rejected at compile time: literals the macros reject, and metadata ids of the wrong kind. Regenerate the expected output with
//! `TRYBUILD=overwrite cargo test -p pges-core --test compile_fail` and review the diff.

#[test]
fn rejected_literals() {
    if !on_pinned_toolchain() {
        return;
    }

    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}

/// The compiler the `.stderr` files were written with: the stable toolchain CI pins in
/// `.github/workflows/ci.yml`. rustc words its errors differently from one release to the next, so on any other
/// compiler, the MSRV (1.94) included, the cases are skipped; bump this with the CI pin and re-bless.
const TOOLCHAIN: &str = "1.97.1";

/// Whether `rustc` (the one cargo runs, `RUSTC` or the rustup toolchain) is [`TOOLCHAIN`].
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
