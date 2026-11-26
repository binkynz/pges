//! Parses and validates the input of `#[derive(Event)]`.

use std::collections::BTreeMap;

use proc_macro2::Span;
use syn::{Token, ext::IdentExt, parse::Parse, spanned::Spanned};

use crate::core_path;

/// An enum that derives `Event`, with every attribute checked.
pub(crate) struct EventEnum {
    pub(crate) ident: syn::Ident,
    /// The path the generated code names `pges-core` by: `crate = "..."`, or else found from the calling
    /// crate's dependencies.
    pub(crate) krate: syn::Path,
    pub(crate) category: syn::LitStr,
    pub(crate) codec: Codec,
    pub(crate) variants: Vec<EventVariant>,
}

/// One variant: one stored event type.
pub(crate) struct EventVariant {
    pub(crate) ident: syn::Ident,
    pub(crate) name: syn::LitStr,
    pub(crate) version: u16,
    /// `upcasters[i]` takes version `i + 1` to version `i + 2`.
    pub(crate) upcasters: Vec<syn::Path>,
    pub(crate) shape: Shape,
}

/// How a variant carries its data.
pub(crate) enum Shape {
    /// `Created { order_id: Uuid }`, encoded as an object of its fields. A unit variant has no fields.
    Struct(Vec<StructField>),
    /// `Created(Created)`, encoded as the inner value.
    Newtype,
}

/// A field of a struct variant.
pub(crate) struct StructField {
    pub(crate) ident: syn::Ident,
    pub(crate) ty: syn::Type,
    pub(crate) serde_attrs: Vec<syn::Attribute>,
}

/// The codec event data is written with.
#[derive(Clone, Copy)]
pub(crate) enum Codec {
    Json,
}

impl EventEnum {
    /// Reads the derive input, collecting every error instead of stopping at the first.
    pub(crate) fn parse(input: &syn::DeriveInput) -> syn::Result<Self> {
        let mut errors = Errors::default();

        let syn::Data::Enum(data) = &input.data else {
            return Err(syn::Error::new(
                input.ident.span(),
                "#[derive(Event)] only supports enums, with one variant per event type",
            ));
        };
        if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
            errors.push(syn::Error::new(
                input.generics.span(),
                "#[derive(Event)] does not support generic enums",
            ));
        }
        if data.variants.is_empty() {
            errors.push(syn::Error::new(
                input.ident.span(),
                "#[derive(Event)] needs at least one variant",
            ));
        }

        reject_serde_attrs(
            &input.attrs,
            "#[serde] on an event enum does not change the stored data; for container attributes such \
             as `rename_all`, use a newtype variant around a struct that derives serde",
            &mut errors,
        );
        let container = errors.collect(parse_container_attrs(&input.attrs));
        let variants: Vec<EventVariant> = data
            .variants
            .iter()
            .filter_map(|variant| errors.collect(parse_variant(variant)))
            .collect();

        check_unique_names(&variants, &mut errors);

        let category = container.as_ref().and_then(|container| {
            let category = container.category.clone();
            if category.is_none() {
                errors.push(syn::Error::new(
                    input.ident.span(),
                    "missing category: add #[event(category = \"...\")] to the enum",
                ));
            }
            category
        });

        errors.finish()?;

        match (category, container) {
            (Some(category), Some(container)) => Ok(Self {
                ident: input.ident.clone(),
                krate: container.krate.unwrap_or_else(core_path::resolve),
                category,
                codec: container.codec,
                variants,
            }),
            _ => Err(syn::Error::new(
                input.ident.span(),
                "invalid #[event] attributes",
            )),
        }
    }
}

struct ContainerAttrs {
    category: Option<syn::LitStr>,
    codec: Codec,
    krate: Option<syn::Path>,
}

/// `1 => path` inside `upcast(...)`.
struct UpcastEntry {
    from: syn::LitInt,
    upcaster: syn::Path,
}

impl Parse for UpcastEntry {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let from = input.parse()?;
        input.parse::<Token![=>]>()?;
        let upcaster = input.parse()?;

        Ok(Self { from, upcaster })
    }
}

/// Errors gathered across the whole input, reported together.
#[derive(Default)]
struct Errors(Option<syn::Error>);

impl Errors {
    fn push(&mut self, error: syn::Error) {
        match &mut self.0 {
            Some(errors) => errors.combine(error),
            None => self.0 = Some(error),
        }
    }

    fn collect<T>(&mut self, result: syn::Result<T>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.push(error);
                None
            }
        }
    }

    fn finish(self) -> syn::Result<()> {
        match self.0 {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn parse_container_attrs(attrs: &[syn::Attribute]) -> syn::Result<ContainerAttrs> {
    let mut errors = Errors::default();
    let mut category: Option<syn::LitStr> = None;
    let mut codec: Option<Codec> = None;
    let mut krate: Option<syn::Path> = None;

    for attr in attrs.iter().filter(|attr| attr.path().is_ident("event")) {
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("category") {
                let value: syn::LitStr = meta.value()?.parse()?;
                if category.is_some() {
                    return Err(meta.error("duplicate `category`"));
                }
                if let Err(reason) = validate_category(&value.value()) {
                    errors.push(syn::Error::new(
                        value.span(),
                        format!("invalid category name {:?}: {reason}", value.value()),
                    ));
                }
                category = Some(value);
                Ok(())
            } else if meta.path.is_ident("codec") {
                let value: syn::LitStr = meta.value()?.parse()?;
                if codec.is_some() {
                    return Err(meta.error("duplicate `codec`"));
                }
                codec = Some(parse_codec(&value)?);
                Ok(())
            } else if meta.path.is_ident("crate") {
                let value: syn::LitStr = meta.value()?.parse()?;
                if krate.is_some() {
                    return Err(meta.error("duplicate `crate`"));
                }
                krate = Some(parse_crate_path(&value)?);
                Ok(())
            } else {
                Err(meta.error("unknown enum attribute; expected `category`, `codec` or `crate`"))
            }
        });
        errors.collect(parsed);
    }

    errors.finish()?;

    Ok(ContainerAttrs {
        category,
        codec: codec.unwrap_or(Codec::Json),
        krate,
    })
}

/// `crate = "pges::core"`: a plain path, without generic arguments, to the `pges-core` API.
fn parse_crate_path(value: &syn::LitStr) -> syn::Result<syn::Path> {
    let path: syn::Path = value.parse().map_err(|_| {
        syn::Error::new(
            value.span(),
            format!(
                "invalid crate path {:?}: expected a path such as \"pges::core\"",
                value.value()
            ),
        )
    })?;
    if path
        .segments
        .iter()
        .any(|segment| !segment.arguments.is_none())
    {
        return Err(syn::Error::new(
            value.span(),
            "the crate path must not have generic arguments",
        ));
    }

    Ok(path)
}

fn parse_codec(value: &syn::LitStr) -> syn::Result<Codec> {
    match value.value().as_str() {
        "json" => Ok(Codec::Json),
        other => Err(syn::Error::new(
            value.span(),
            format!("unsupported codec {other:?}; supported codecs: \"json\""),
        )),
    }
}

fn parse_variant(variant: &syn::Variant) -> syn::Result<EventVariant> {
    let mut errors = Errors::default();
    let mut name: Option<syn::LitStr> = None;
    let mut version: Option<(u16, syn::LitInt)> = None;
    let mut upcast: Option<(Span, Vec<UpcastEntry>)> = None;

    for attr in variant
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("event"))
    {
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value: syn::LitStr = meta.value()?.parse()?;
                if name.is_some() {
                    return Err(meta.error("duplicate `name`"));
                }
                if let Err(reason) = validate_type_name(&value.value()) {
                    errors.push(syn::Error::new(value.span(), reason));
                }
                name = Some(value);
                Ok(())
            } else if meta.path.is_ident("version") {
                let value: syn::LitInt = meta.value()?.parse()?;
                if version.is_some() {
                    return Err(meta.error("duplicate `version`"));
                }
                let number: u16 = value.base10_parse()?;
                if number == 0 {
                    return Err(syn::Error::new(value.span(), "schema versions start at 1"));
                }
                version = Some((number, value));
                Ok(())
            } else if meta.path.is_ident("upcast") {
                if upcast.is_some() {
                    return Err(meta.error("duplicate `upcast`"));
                }
                let content;
                syn::parenthesized!(content in meta.input);
                let entries = content.parse_terminated(UpcastEntry::parse, Token![,])?;
                upcast = Some((meta.path.span(), entries.into_iter().collect()));
                Ok(())
            } else {
                Err(meta.error("unknown variant attribute; expected `name`, `version` or `upcast`"))
            }
        });
        errors.collect(parsed);
    }

    reject_serde_attrs(
        &variant.attrs,
        "#[serde] on a variant does not change the stored data; for container attributes such as \
         `rename_all`, use a newtype variant around a struct that derives serde",
        &mut errors,
    );
    let shape = errors.collect(parse_shape(variant));

    if name.is_none() {
        errors.push(syn::Error::new(
            variant.ident.span(),
            format!(
                "missing event name: add #[event(name = \"...\")] to variant `{}`",
                variant.ident
            ),
        ));
    }

    let current = version.as_ref().map_or(1, |(number, _)| *number);
    let upcasters = match upcast {
        Some((span, entries)) => errors.collect(upcast_chain(span, current, entries)),
        None => {
            let span = version
                .as_ref()
                .map_or(variant.ident.span(), |(_, literal)| literal.span());
            errors.collect(upcast_chain(span, current, Vec::new()))
        }
    };

    errors.finish()?;

    match (name, shape, upcasters) {
        (Some(name), Some(shape), Some(upcasters)) => Ok(EventVariant {
            ident: variant.ident.clone(),
            name,
            version: current,
            upcasters,
            shape,
        }),
        _ => Err(syn::Error::new(
            variant.ident.span(),
            "invalid #[event] attributes",
        )),
    }
}

fn parse_shape(variant: &syn::Variant) -> syn::Result<Shape> {
    match &variant.fields {
        syn::Fields::Named(fields) => {
            let mut errors = Errors::default();
            let mut stored = StoredNames::new();
            let mut struct_fields = Vec::with_capacity(fields.named.len());

            for field in &fields.named {
                let Some(ident) = field.ident.clone() else {
                    continue;
                };
                let serde_attrs: Vec<syn::Attribute> = field
                    .attrs
                    .iter()
                    .filter(|attr| attr.path().is_ident("serde"))
                    .cloned()
                    .collect();

                if let Some(serde) = errors.collect(FieldSerde::parse(&serde_attrs)) {
                    check_stored_names(&ident, &serde, &mut stored, &mut errors);
                }
                struct_fields.push(StructField {
                    ident,
                    ty: field.ty.clone(),
                    serde_attrs,
                });
            }

            errors.finish()?;

            Ok(Shape::Struct(struct_fields))
        }
        syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
            let mut errors = Errors::default();
            for field in &fields.unnamed {
                reject_serde_attrs(
                    &field.attrs,
                    "#[serde] on a newtype variant's field does not change the stored data; put serde \
                     attributes on the inner type",
                    &mut errors,
                );
            }

            errors.finish()?;

            Ok(Shape::Newtype)
        }
        syn::Fields::Unnamed(fields) => Err(syn::Error::new(
            fields.span(),
            "tuple variants must have exactly one field; use a struct variant or a newtype variant",
        )),
        syn::Fields::Unit => Ok(Shape::Struct(Vec::new())),
    }
}

/// The names a struct variant's fields are stored and read under so far, with the field that uses each.
type StoredNames = BTreeMap<String, syn::Ident>;

/// What a field's `#[serde(...)]` attributes do to the stored object. Only attributes that write data the
/// same decode can read back are accepted.
#[derive(Default)]
struct FieldSerde {
    rename: Option<syn::LitStr>,
    aliases: Vec<syn::LitStr>,
    default: Option<Span>,
    skip: Option<Span>,
    skip_serializing: Option<(Span, &'static str)>,
    serialize_with: Option<Span>,
    deserialize_with: Option<Span>,
    flatten: Option<Span>,
}

impl FieldSerde {
    fn parse(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut errors = Errors::default();
        let mut serde = Self::default();

        for attr in attrs {
            let parsed = attr.parse_nested_meta(|meta| serde.parse_meta(&meta, &mut errors));
            errors.collect(parsed);
        }
        serde.check(&mut errors);

        errors.finish()?;

        Ok(serde)
    }

    fn parse_meta(
        &mut self,
        meta: &syn::meta::ParseNestedMeta<'_>,
        errors: &mut Errors,
    ) -> syn::Result<()> {
        let span = meta.path.span();
        let key = meta
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default();

        match key.as_str() {
            "rename" => {
                let rename = parse_rename(meta)?;
                if self.rename.replace(rename).is_some() {
                    return Err(meta.error("duplicate `rename`"));
                }
            }
            "alias" => self.aliases.push(meta.value()?.parse()?),
            "default" => {
                if meta.input.peek(Token![=]) {
                    meta.value()?.parse::<syn::LitStr>()?;
                }
                self.default = Some(span);
            }
            "skip" => self.skip = Some(span),
            "skip_serializing" => self.skip_serializing = Some((span, "skip_serializing")),
            "skip_serializing_if" => {
                meta.value()?.parse::<syn::LitStr>()?;
                self.skip_serializing = Some((span, "skip_serializing_if"));
            }
            "with" => {
                meta.value()?.parse::<syn::LitStr>()?;
                self.serialize_with = Some(span);
                self.deserialize_with = Some(span);
            }
            "serialize_with" => {
                meta.value()?.parse::<syn::LitStr>()?;
                self.serialize_with = Some(span);
            }
            "deserialize_with" => {
                meta.value()?.parse::<syn::LitStr>()?;
                self.deserialize_with = Some(span);
            }
            "flatten" => self.flatten = Some(span),
            "skip_deserializing" => {
                skip_value(meta)?;
                errors.push(meta.error(
                    "`skip_deserializing` writes the field but never reads it back; use `skip` to \
                     leave it out of the stored data",
                ));
            }
            _ => {
                skip_value(meta)?;
                errors.push(meta.error(
                    "unsupported serde field attribute in an event; supported: `rename`, `alias`, \
                     `default`, `skip`, `skip_serializing` or `skip_serializing_if` with `default`, \
                     `with`, `serialize_with` with `deserialize_with`, and `flatten`",
                ));
            }
        }

        Ok(())
    }

    /// Rejects combinations that write events the same code cannot decode.
    fn check(&self, errors: &mut Errors) {
        if let Some((span, key)) = self.skip_serializing
            && self.default.is_none()
            && self.skip.is_none()
        {
            errors.push(syn::Error::new(
                span,
                format!(
                    "`{key}` without `default` can write events that cannot be read back: add \
                     `default` to the same field"
                ),
            ));
        }

        match (self.serialize_with, self.deserialize_with) {
            (Some(span), None) => errors.push(syn::Error::new(
                span,
                "`serialize_with` without `deserialize_with` writes data the default decoder may not \
                 read: add `deserialize_with`, or use `with` for a module with both",
            )),
            (None, Some(span)) => errors.push(syn::Error::new(
                span,
                "`deserialize_with` without `serialize_with` reads a shape the default encoder does \
                 not write: add `serialize_with`, or use `with` for a module with both",
            )),
            _ => {}
        }
    }

    /// The names the field is stored and read under; none when it is skipped or flattened.
    fn stored_names(&self, ident: &syn::Ident) -> Vec<(String, Span)> {
        if self.skip.is_some() || self.flatten.is_some() {
            return Vec::new();
        }

        let name = match &self.rename {
            Some(rename) => (rename.value(), rename.span()),
            None => (ident.unraw().to_string(), ident.span()),
        };

        std::iter::once(name)
            .chain(
                self.aliases
                    .iter()
                    .map(|alias| (alias.value(), alias.span())),
            )
            .collect()
    }
}

/// `rename = "x"`, or `rename(serialize = "x", deserialize = "x")` with the same name on both sides.
fn parse_rename(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<syn::LitStr> {
    if meta.input.peek(Token![=]) {
        return meta.value()?.parse();
    }

    let mut serialize: Option<syn::LitStr> = None;
    let mut deserialize: Option<syn::LitStr> = None;
    meta.parse_nested_meta(|side| {
        if side.path.is_ident("serialize") {
            serialize = Some(side.value()?.parse()?);
            Ok(())
        } else if side.path.is_ident("deserialize") {
            deserialize = Some(side.value()?.parse()?);
            Ok(())
        } else {
            Err(side.error("expected `serialize` or `deserialize`"))
        }
    })?;

    match (serialize, deserialize) {
        (Some(serialize), Some(deserialize)) if serialize.value() == deserialize.value() => {
            Ok(serialize)
        }
        _ => Err(meta.error(
            "a rename that differs between serialize and deserialize writes events that cannot be \
             read back; use `rename = \"...\"`, and `alias` for more names to read",
        )),
    }
}

/// Consumes the value of an attribute the derive rejects, so the attributes after it are still read.
fn skip_value(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if meta.input.peek(Token![=]) {
        meta.value()?.parse::<syn::Expr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        content.parse::<proc_macro2::TokenStream>()?;
    }

    Ok(())
}

/// Two fields stored or read under one name write an object that fails to decode with a duplicate field.
fn check_stored_names(
    ident: &syn::Ident,
    serde: &FieldSerde,
    stored: &mut StoredNames,
    errors: &mut Errors,
) {
    for (name, span) in serde.stored_names(ident) {
        match stored.get(&name) {
            Some(first) if first != ident => errors.push(syn::Error::new(
                span,
                format!(
                    "field `{ident}` is stored or read as {name:?}, which field `{first}` already uses"
                ),
            )),
            Some(_) => errors.push(syn::Error::new(
                span,
                format!("field `{ident}` lists the name {name:?} twice"),
            )),
            None => {
                stored.insert(name, ident.clone());
            }
        }
    }
}

/// `#[serde]` where the derive does not pass it on to the stored data.
fn reject_serde_attrs(attrs: &[syn::Attribute], message: &str, errors: &mut Errors) {
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        errors.push(syn::Error::new_spanned(attr, message));
    }
}

/// Orders the upcasters by the version they start from, and checks they cover `1..current` exactly.
fn upcast_chain(
    span: Span,
    current: u16,
    entries: Vec<UpcastEntry>,
) -> syn::Result<Vec<syn::Path>> {
    let mut errors = Errors::default();
    let mut chain: BTreeMap<u16, syn::Path> = BTreeMap::new();

    for entry in entries {
        let Some(from) = errors.collect(entry.from.base10_parse::<u16>()) else {
            continue;
        };

        if from == 0 || from >= current {
            errors.push(syn::Error::new(
                entry.from.span(),
                format!(
                    "upcast entry {from} is out of range: {}",
                    expected_upcasters(current)
                ),
            ));
        } else if chain.insert(from, entry.upcaster).is_some() {
            errors.push(syn::Error::new(
                entry.from.span(),
                format!("duplicate upcast entry for version {from}"),
            ));
        }
    }

    let missing: Vec<String> = (1..current)
        .filter(|version| !chain.contains_key(version))
        .map(|version| version.to_string())
        .collect();
    if !missing.is_empty() {
        let plural = if missing.len() == 1 { "" } else { "s" };
        errors.push(syn::Error::new(
            span,
            format!(
                "missing upcaster for version{plural} {}: {}",
                missing.join(", "),
                expected_upcasters(current)
            ),
        ));
    }

    errors.finish()?;

    Ok(chain.into_values().collect())
}

fn expected_upcasters(current: u16) -> String {
    match current {
        1 => "version 1 has no older versions to upcast".to_owned(),
        2 => "version 2 needs upcast(1 => ...)".to_owned(),
        3 => "version 3 needs upcast(1 => ..., 2 => ...)".to_owned(),
        _ => format!(
            "version {current} needs upcast(1 => ..., ..., {} => ...)",
            current - 1
        ),
    }
}

fn check_unique_names(variants: &[EventVariant], errors: &mut Errors) {
    let mut seen: BTreeMap<String, &syn::Ident> = BTreeMap::new();

    for variant in variants {
        let name = variant.name.value();
        match seen.get(&name) {
            Some(first) => errors.push(syn::Error::new(
                variant.name.span(),
                format!("duplicate event name {name:?}: variant `{first}` already uses it"),
            )),
            None => {
                seen.insert(name, &variant.ident);
            }
        }
    }
}

/// The rule of `pges_core::category::CategoryName`: `^[a-z][a-z0-9_]{0,39}$`.
fn validate_category(name: &str) -> Result<(), &'static str> {
    let mut bytes = name.bytes();

    match bytes.next() {
        None => return Err("it is empty"),
        Some(first) if !first.is_ascii_lowercase() => {
            return Err("it must start with a lowercase ASCII letter");
        }
        Some(_) => {}
    }
    if name.len() > 40 {
        return Err("it is longer than 40 bytes");
    }
    if !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_') {
        return Err("it may only contain a-z, 0-9 and _");
    }

    Ok(())
}

fn validate_type_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("event names must not be empty".to_owned());
    }
    if name.starts_with('$') {
        return Err(format!(
            "invalid event name {name:?}: names starting with `$` are reserved for system events"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_rule_matches_pges_core() {
        assert!(validate_category("jobs").is_ok());
        assert!(validate_category("job_items_2").is_ok());
        assert!(validate_category(&"a".repeat(40)).is_ok());
        assert!(validate_category("").is_err());
        assert!(validate_category("Jobs").is_err());
        assert!(validate_category("2jobs").is_err());
        assert!(validate_category("jobs-x").is_err());
        assert!(validate_category(&"a".repeat(41)).is_err());
    }

    #[test]
    fn type_names_reject_the_system_prefix() {
        assert!(validate_type_name("job.created").is_ok());
        assert!(validate_type_name("$stream-deleted").is_err());
        assert!(validate_type_name("").is_err());
    }
}
