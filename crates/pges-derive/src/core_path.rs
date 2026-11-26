//! Finds the path the generated code names `pges-core` by, from the calling crate's `Cargo.toml`.

use proc_macro_crate::FoundCrate;
use proc_macro2::Span;

/// The path to `pges-core`'s root module in the crate being compiled.
///
/// A `pges` dependency wins over a `pges-core` one, under whatever name the crate gave it. Without either,
/// or when the manifest cannot be read, the path is `::pges_core`.
pub(crate) fn resolve() -> syn::Path {
    from_found(
        proc_macro_crate::crate_name("pges").ok(),
        proc_macro_crate::crate_name("pges-core").ok(),
    )
}

/// `facade` is how the crate names `pges`, `core` how it names `pges-core`, each if it depends on it.
fn from_found(facade: Option<FoundCrate>, core: Option<FoundCrate>) -> syn::Path {
    match (facade, core) {
        // `Itself` is reported for the facade's own library, unit tests and doc examples (its integration
        // tests get `Name`). The library and unit tests do not use the derive; doc examples name it `::pges`.
        (Some(FoundCrate::Itself), _) => syn::parse_quote!(::pges::core),
        (Some(FoundCrate::Name(name)), _) => {
            let name = syn::Ident::new(&name, Span::call_site());
            syn::parse_quote!(::#name::core)
        }
        // `Itself` for pges-core is reported for its library, unit tests and doc examples. `crate` would
        // be wrong in doc examples, which are crates of their own; `::pges_core` resolves there, and in
        // the library too once it declares `extern crate self as pges_core;` (it does not use the derive).
        (None, Some(FoundCrate::Itself)) | (None, None) => syn::parse_quote!(::pges_core),
        (None, Some(FoundCrate::Name(name))) => {
            let name = syn::Ident::new(&name, Span::call_site());
            syn::parse_quote!(::#name)
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::ToTokens;

    use super::*;

    fn path(facade: Option<FoundCrate>, core: Option<FoundCrate>) -> String {
        from_found(facade, core)
            .to_token_stream()
            .to_string()
            .replace(' ', "")
    }

    fn named(name: &str) -> Option<FoundCrate> {
        Some(FoundCrate::Name(name.to_owned()))
    }

    #[test]
    fn the_facade_wins_under_any_name() {
        assert_eq!(path(named("pges"), None), "::pges::core");
        assert_eq!(path(named("events"), None), "::events::core");
        assert_eq!(path(named("pges"), named("pges_core")), "::pges::core");
        assert_eq!(
            path(Some(FoundCrate::Itself), named("pges_core")),
            "::pges::core"
        );
    }

    #[test]
    fn pges_core_is_used_under_its_name() {
        assert_eq!(path(None, named("pges_core")), "::pges_core");
        assert_eq!(path(None, named("store_core")), "::store_core");
        assert_eq!(path(None, Some(FoundCrate::Itself)), "::pges_core");
    }

    #[test]
    fn without_either_the_path_is_pges_core() {
        assert_eq!(path(None, None), "::pges_core");
    }
}
