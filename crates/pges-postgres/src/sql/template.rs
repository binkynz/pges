//! Statement templates: what a crate's statement registry is made of, and the renderer.
//!
//! A statement's SQL lives in `.sql` files under the crate's `sql/` directory. Each file starts with a header
//! of `--` comment lines, which rendering drops. The rest may name placeholders (`{store}`, `{category}`, ...,
//! see [`Placeholder`]) that rendering replaces with [`Values`]. Every value is a validated identifier or a
//! literal built from typed values, never text from a caller.

use std::{fmt, str::FromStr};

use sqlx::{AssertSqlSafe, SqlSafeStr, SqlStr};

use pges_core::{category, store};

use crate::ident;

/// Rendered registry SQL as sqlx runs it as text through an `Executor` (DDL, and statements without parameters
/// whose rows are ignored). sqlx accepts only `&'static str` without an assertion that the text is free of
/// injected input; rendered SQL is, because every placeholder value is a validated identifier or a literal built
/// from typed values (module docs). A [`Statement::sql`] is `&'static str` and needs no conversion.
///
/// It asserts without checking: pass only SQL rendered from the registry. Within this crate,
/// [`Statement::render_text`] is the only caller; pges-testing and the bench use it for SQL they render and
/// keep as a `String`. Not part of the API.
#[doc(hidden)]
pub fn text(sql: impl Into<String>) -> SqlStr {
    AssertSqlSafe(sql.into()).into_sql_str()
}

/// One statement a crate runs, made of one or more `.sql` files.
#[derive(Debug)]
pub struct Statement {
    /// The statement's name, unique within its crate's registry.
    pub name: &'static str,
    /// The files whose SQL, joined by line breaks, makes the statement. A variant can share fragments with
    /// another: the append statement with and without the outbox share their first file.
    pub files: &'static [File],
    /// Whether PostgreSQL can describe the statement.
    pub kind: Kind,
    /// The PostgreSQL types of `$1`, `$2`, ..., as sqlx sends them: for a query, those of its parameters' Rust
    /// types (`typed_query!`).
    pub params: &'static [PgType],
    /// The result columns, in order. Empty for a statement that returns no rows.
    pub columns: &'static [Column],
}

impl Statement {
    /// The statement's SQL with `values` filled in. A placeholder without a value, or with an unknown name,
    /// is left as it is, so the statement fails when it runs; the registry test catches both.
    pub fn render(&self, values: &Values) -> String {
        let mut sql = String::new();
        for (i, file) in self.files.iter().enumerate() {
            if i > 0 {
                sql.push('\n');
            }
            render_into(&mut sql, body(file.text), values);
        }

        sql
    }

    /// [`render`](Self::render) as [`text`], to run through an `Executor`.
    pub fn render_text(&self, values: &Values) -> SqlStr {
        text(self.render(values))
    }

    /// The SQL of a statement with one file and no placeholders, without allocating.
    pub fn sql(&self) -> &'static str {
        self.files.first().map_or("", |file| body(file.text))
    }
}

/// One `.sql` file of a statement.
#[derive(Debug)]
pub struct File {
    /// The path below the crate's `sql/` directory, such as `append/head.sql`.
    pub path: &'static str,
    /// The file's text, from `include_str!`.
    pub text: &'static str,
}

impl File {
    /// The file's SQL with `values` filled in, for a statement whose fragments are composed in code, such as
    /// one repeated per store. Placeholders are handled as by [`Statement::render`].
    pub fn render(&self, values: &Values) -> String {
        let mut sql = String::new();
        render_into(&mut sql, body(self.text), values);

        sql
    }
}

/// Whether PostgreSQL can describe a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A query or DML statement, declared with `typed_query!`: the registry test checks its parameters' and
    /// columns' Rust types with `describe`.
    Query,
    /// DDL, or a utility command such as `SET` or `VACUUM`, which cannot be prepared: the registry test runs
    /// it instead. It takes no parameters and returns no rows.
    Ddl,
}

/// A PostgreSQL type, by the name sqlx reports for it (`TypeInfo::name`): upper case for built-in types, with
/// `[]` for arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgType(&'static str);

impl PgType {
    /// `bool`.
    pub const BOOL: Self = Self("BOOL");
    /// `int2`.
    pub const INT2: Self = Self("INT2");
    /// `int4`.
    pub const INT4: Self = Self("INT4");
    /// `int8`.
    pub const INT8: Self = Self("INT8");
    /// `float8`.
    pub const FLOAT8: Self = Self("FLOAT8");
    /// `text`.
    pub const TEXT: Self = Self("TEXT");
    /// `bytea`.
    pub const BYTEA: Self = Self("BYTEA");
    /// `uuid`.
    pub const UUID: Self = Self("UUID");
    /// `interval`.
    pub const INTERVAL: Self = Self("INTERVAL");
    /// `void`, the result of functions such as `pg_advisory_lock`.
    pub const VOID: Self = Self("VOID");
    /// `int2[]`.
    pub const INT2_ARRAY: Self = Self("INT2[]");
    /// `int4[]`.
    pub const INT4_ARRAY: Self = Self("INT4[]");
    /// `int8[]`.
    pub const INT8_ARRAY: Self = Self("INT8[]");
    /// `text[]`.
    pub const TEXT_ARRAY: Self = Self("TEXT[]");
    /// `bytea[]`.
    pub const BYTEA_ARRAY: Self = Self("BYTEA[]");
    /// `uuid[]`.
    pub const UUID_ARRAY: Self = Self("UUID[]");

    /// Any other type, by the name sqlx reports for it, such as `PG_LSN`.
    pub const fn named(name: &'static str) -> Self {
        Self(name)
    }

    /// The name sqlx reports for the type.
    pub const fn name(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for PgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// A result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    /// The column's name, as PostgreSQL reports it (`?column?` for an unnamed expression).
    pub name: &'static str,
    /// The column's type.
    pub ty: PgType,
}

impl Column {
    /// A column named `name` of type `ty`.
    pub const fn new(name: &'static str, ty: PgType) -> Self {
        Self { name, ty }
    }
}

/// The placeholders a template can name, written `{name}` in the `.sql` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumString, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum Placeholder {
    /// The store's schema: a [`store::StoreName`].
    Store,
    /// A category's name: a [`category::CategoryName`]. Its partition of `commits` is `commits_{category}`.
    Category,
    /// A category's id: a [`category::CategoryId`], as an integer literal.
    CategoryId,
    /// A time leaf of a category, such as `commits_jobs__202611`.
    Leaf,
    /// A log partition, such as `log_0000`.
    LogPartition,
    /// A partition of either kind, in statements shared by leaves and log partitions.
    Table,
    /// The parent of a [`Placeholder::Table`]: `commits_<category>` or `log`.
    Parent,
    /// An index name.
    Index,
    /// A constraint name.
    Bound,
    /// A partition's lower bound: a [`Literal`].
    From,
    /// A partition's upper bound: a [`Literal`].
    To,
    /// A snapshot exported by another transaction: a [`SnapshotName`], quoted by the template.
    Snapshot,
    /// A database's name.
    Database,
    /// The name of a template database.
    Template,
    /// The table an append writes a stream's head to: `streams`, or the hash partition of it that holds the
    /// stream, such as `streams_p017`. Set with [`Values::stream_table`].
    StreamTable,
}

/// The placeholders that take an [`Ident`], set with [`Values::ident`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum IdentPlaceholder {
    /// [`Placeholder::Leaf`].
    Leaf,
    /// [`Placeholder::LogPartition`].
    LogPartition,
    /// [`Placeholder::Table`].
    Table,
    /// [`Placeholder::Parent`].
    Parent,
    /// [`Placeholder::Index`].
    Index,
    /// [`Placeholder::Bound`].
    Bound,
    /// [`Placeholder::Database`].
    Database,
    /// [`Placeholder::Template`].
    Template,
}

impl From<IdentPlaceholder> for Placeholder {
    fn from(placeholder: IdentPlaceholder) -> Self {
        match placeholder {
            IdentPlaceholder::Leaf => Self::Leaf,
            IdentPlaceholder::LogPartition => Self::LogPartition,
            IdentPlaceholder::Table => Self::Table,
            IdentPlaceholder::Parent => Self::Parent,
            IdentPlaceholder::Index => Self::Index,
            IdentPlaceholder::Bound => Self::Bound,
            IdentPlaceholder::Database => Self::Database,
            IdentPlaceholder::Template => Self::Template,
        }
    }
}

/// The values of a statement's placeholders. Each setter takes a validated or typed value.
#[derive(Debug, Clone, Default)]
pub struct Values {
    entries: Vec<(Placeholder, String)>,
}

impl Values {
    /// No values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `{store}`.
    pub fn store(self, store: &store::StoreName) -> Self {
        self.with(Placeholder::Store, store.as_str().to_owned())
    }

    /// Sets `{category}`.
    pub fn category(self, category: category::CategoryName) -> Self {
        self.with(Placeholder::Category, category.as_str().to_owned())
    }

    /// Sets `{category_id}`.
    pub fn category_id(self, id: category::CategoryId) -> Self {
        self.with(Placeholder::CategoryId, id.get().to_string())
    }

    /// Sets `{stream_table}`: `streams` for `None`, otherwise the partition of `streams` named.
    pub fn stream_table(self, partition: Option<&Ident>) -> Self {
        let table = partition.map_or("streams", Ident::as_str);

        self.with(Placeholder::StreamTable, table.to_owned())
    }

    /// Sets an identifier placeholder.
    pub fn ident(self, placeholder: IdentPlaceholder, ident: &Ident) -> Self {
        self.with(placeholder.into(), ident.as_str().to_owned())
    }

    /// Sets `{from}` and `{to}`.
    pub fn range(self, from: &Literal, to: &Literal) -> Self {
        self.with(Placeholder::From, from.0.clone())
            .with(Placeholder::To, to.0.clone())
    }

    /// Sets `{snapshot}`.
    pub fn snapshot(self, snapshot: &SnapshotName) -> Self {
        self.with(Placeholder::Snapshot, snapshot.0.clone())
    }

    fn with(mut self, placeholder: Placeholder, value: String) -> Self {
        self.entries.retain(|(p, _)| *p != placeholder);
        self.entries.push((placeholder, value));

        self
    }

    fn get(&self, placeholder: Placeholder) -> Option<&str> {
        self.entries
            .iter()
            .find(|(p, _)| *p == placeholder)
            .map(|(_, value)| value.as_str())
    }
}

/// An identifier that needs no quoting: `^[a-z][a-z0-9_]*$`, at most 63 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident(String);

impl Ident {
    /// Validates `name`; `None` if it needs quoting or would be truncated.
    pub fn new(name: impl Into<String>) -> Option<Self> {
        let name = name.into();

        ident::is_plain_identifier(&name).then_some(Self(name))
    }

    /// The identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A literal interpolated into DDL, which takes no bind parameters: an integer, or `timestamptz` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal(String);

impl Literal {
    /// An integer.
    pub fn int(value: i64) -> Self {
        Self(value.to_string())
    }

    /// `timestamptz` text as PostgreSQL prints it in any `DateStyle`, such as `2026-11-01 00:00:00+00`, for
    /// a template that quotes it. `None` unless it holds only ASCII letters, digits, spaces and `-:.+/,`, so it
    /// cannot end the quoted literal.
    pub fn timestamptz(text: &str) -> Option<Self> {
        let plain = !text.is_empty()
            && text.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || matches!(b, b' ' | b'-' | b':' | b'.' | b'+' | b'/' | b',')
            });

        plain.then(|| Self(text.to_owned()))
    }
}

/// The name of an exported snapshot, such as `00000003-0000001B-1`: 1 to 63 characters from `0-9`, `A-F`,
/// `a-f` and `-`, so it cannot end the quoted literal it is put in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotName(String);

impl SnapshotName {
    /// Validates `name`; `None` if it is not a snapshot name.
    pub fn new(name: &str) -> Option<Self> {
        let valid = (1..=63).contains(&name.len())
            && name.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-');

        valid.then(|| Self(name.to_owned()))
    }
}

/// The placeholders (`{name}`) left in rendered SQL: unknown names, and known ones without a value.
pub fn leftover_placeholders(sql: &str) -> Vec<&str> {
    let mut found = Vec::new();
    let mut rest = sql;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let len = placeholder_len(after);
        if len > 0 && after.as_bytes().get(len) == Some(&b'}') {
            found.push(&after[..len]);
        }
        rest = after;
    }

    found
}

/// The placeholder names a file's SQL uses, known or not, in order of appearance.
pub fn placeholders(file: &File) -> Vec<&'static str> {
    leftover_placeholders(body(file.text))
}

/// The SQL of a file without its header: the leading `--` comment lines and blank lines.
pub fn body(text: &'static str) -> &'static str {
    let mut rest = text;
    loop {
        let line_end = rest.find('\n').map_or(rest.len(), |i| i + 1);
        let line = rest[..line_end].trim();
        if !(line.is_empty() || line.starts_with("--")) || rest.is_empty() {
            return rest.trim_end();
        }
        rest = &rest[line_end..];
    }
}

fn render_into(sql: &mut String, text: &str, values: &Values) {
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        sql.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let len = placeholder_len(after);
        let value = (len > 0 && after.as_bytes().get(len) == Some(&b'}'))
            .then(|| Placeholder::from_str(&after[..len]).ok())
            .flatten()
            .and_then(|placeholder| values.get(placeholder));
        match value {
            Some(value) => {
                sql.push_str(value);
                rest = &after[len + 1..];
            }
            None => {
                sql.push('{');
                rest = after;
            }
        }
    }
    sql.push_str(rest);
}

fn placeholder_len(text: &str) -> usize {
    text.bytes()
        .take_while(|b| b.is_ascii_lowercase() || *b == b'_')
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    const FILE: File = File {
        path: "test/one.sql",
        text: "-- A header.\n--\n-- $1 an id.\n\nSELECT * FROM {store}.commits_{category}\nWHERE x = {nope} AND y = '{}'\n",
    };
    const STATEMENT: Statement = Statement {
        name: "test/one",
        files: &[FILE],
        kind: Kind::Query,
        params: &[],
        columns: &[],
    };

    fn jobs() -> category::CategoryName {
        pges_core::category!("jobs")
    }

    #[test]
    fn drops_the_header_and_fills_known_placeholders() {
        let store = pges_core::store_name!("ops_es");
        let values = Values::new().store(&store).category(jobs());

        let sql = STATEMENT.render(&values);

        assert_eq!(
            sql,
            "SELECT * FROM ops_es.commits_jobs\nWHERE x = {nope} AND y = '{}'"
        );
        assert_eq!(leftover_placeholders(&sql), ["nope"]);
        assert_eq!(placeholders(&FILE), ["store", "category", "nope"]);
    }

    #[test]
    fn leaves_placeholders_without_a_value() {
        let sql = STATEMENT.render(&Values::new().category(jobs()));

        assert_eq!(leftover_placeholders(&sql), ["store", "nope"]);
    }

    #[test]
    fn joins_files_with_line_breaks() {
        const TWO: Statement = Statement {
            name: "test/two",
            files: &[
                File {
                    path: "a.sql",
                    text: "-- a\nWITH a AS (SELECT 1)",
                },
                File {
                    path: "b.sql",
                    text: "-- b\nSELECT * FROM a\n",
                },
            ],
            kind: Kind::Query,
            params: &[],
            columns: &[],
        };

        assert_eq!(
            TWO.render(&Values::new()),
            "WITH a AS (SELECT 1)\nSELECT * FROM a"
        );
    }

    #[test]
    fn values_are_validated() {
        assert!(store::StoreName::new("pges").is_err());
        assert!(store::StoreName::new("ops;drop").is_err());
        assert!(Ident::new("commits_jobs__202611").is_some());
        assert!(Ident::new("Commits").is_none());
        assert!(Literal::timestamptz("2026-11-01 00:00:00+00").is_some());
        assert!(Literal::timestamptz("Sun Nov 01 00:00:00 2026 UTC").is_some());
        assert!(Literal::timestamptz("2026-11-01'); drop").is_none());
        assert!(Literal::timestamptz("2026-11-01\\").is_none());
        assert!(Literal::timestamptz("").is_none());
        assert!(SnapshotName::new("00000003-0000001B-1").is_some());
        assert!(SnapshotName::new("0000-'x").is_none());
        assert!(SnapshotName::new("").is_none());
        assert!(SnapshotName::new(&"0".repeat(64)).is_none());
    }

    #[test]
    fn renders_one_file() {
        let store = pges_core::store_name!("ops_es");
        let values = Values::new().store(&store).category(jobs());

        assert_eq!(
            FILE.render(&values),
            "SELECT * FROM ops_es.commits_jobs\nWHERE x = {nope} AND y = '{}'"
        );
    }

    #[test]
    fn ident_placeholders_keep_their_names() {
        for placeholder in <IdentPlaceholder as strum::IntoEnumIterator>::iter() {
            let name: &str = placeholder.into();
            let general: &str = Placeholder::from(placeholder).into();
            assert_eq!(name, general);
        }
    }

    #[test]
    fn placeholder_names_are_snake_case() {
        assert_eq!(
            Placeholder::from_str("log_partition").ok(),
            Some(Placeholder::LogPartition)
        );
        let name: &str = Placeholder::CategoryId.into();
        assert_eq!(name, "category_id");
    }
}
