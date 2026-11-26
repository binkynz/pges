//! Typed statements: a registry entry whose parameters and result columns are Rust types, so a call site that
//! binds or reads the wrong type does not compile.
//!
//! [`typed_query!`](crate::typed_query) declares one: the [`template::Statement`] constant, and a module with
//! the statement's [`Query`] marker, its `Params` struct (fields bound as `$1`, `$2`, ... in order) and its
//! `Row` struct (fields read from the columns of the same names). The PostgreSQL types of the constant come
//! from the Rust types through [`Value`]. A call site renders the statement into a [`Rendered`], binds a
//! `Params` with [`Rendered::bind`] and runs the [`Bound`] query, which returns `Row`s.
//!
//! The registry test (`pges_testing::statement::check_typed`) describes every typed statement on PostgreSQL 18
//! and checks that each Rust type accepts the type PostgreSQL reports, and that a column read as a non-`Option`
//! type is not nullable.

use std::{borrow::Cow, fmt, marker::PhantomData, sync::Arc};

use sqlx::{
    AssertSqlSafe, Executor, Postgres, SqlSafeStr, SqlStr,
    postgres::{
        PgArgumentBuffer, PgArguments, PgQueryResult, PgRow, PgTypeInfo,
        types::{Oid, PgInterval},
    },
};
use uuid::Uuid;

use crate::sql::template;

/// A query with its arguments bound, as sqlx runs it.
pub type Arguments<'q> = sqlx::query::Query<'q, Postgres, PgArguments>;

/// A typed statement: its registry entry, its parameters and its rows. Implemented by
/// [`typed_query!`](crate::typed_query).
pub trait Query: Sized + 'static {
    /// The registry entry.
    const STATEMENT: &'static template::Statement;

    /// The parameters, bound in order.
    type Params<'q>: Params<'q>;

    /// One result row. Borrows from the row sqlx returned when it holds a `&[u8]` or `&str`.
    type Row<'r>: Decode<'r>;

    /// The statement's SQL with `values` filled in.
    fn render(values: &template::Values) -> Rendered<Self> {
        Rendered::new(Self::STATEMENT.render(values))
    }

    /// The SQL of a statement of one file without placeholders ([`template::Statement::sql`]).
    fn fixed() -> Rendered<Self> {
        Rendered::new(Self::STATEMENT.sql())
    }
}

/// A typed statement whose rows borrow nothing, so they can be returned from the query.
pub trait Owned: Query {
    /// The row.
    type Owned: for<'r> Decode<'r>;
}

/// A statement with the parameters and rows of `S`, such as the append with the outbox: its SQL can be
/// rendered as a [`Rendered<S>`] with [`Rendered::variant`]. Implemented by
/// [`typed_query!`](crate::typed_query) for a statement declared `like` another.
pub trait Variant<S: Query>:
    for<'q, 'r> Query<Params<'q> = S::Params<'q>, Row<'r> = S::Row<'r>>
{
}

/// The parameters of a typed statement.
pub trait Params<'q> {
    /// The PostgreSQL type of each parameter, from its Rust type.
    const TYPES: &'static [template::PgType];

    /// Binds the fields in order.
    fn bind(self, query: Arguments<'q>) -> Arguments<'q>;

    /// The Rust type of each parameter, for the registry test.
    fn types() -> Vec<Type>;
}

/// One result row of a typed statement.
pub trait Decode<'r>: Sized {
    /// The result columns, with the PostgreSQL types of their Rust types.
    const COLUMNS: &'static [template::Column];

    /// Reads the row's columns by name.
    fn decode(row: &'r PgRow) -> Result<Self, sqlx::Error>;

    /// The Rust type of each column, for the registry test.
    fn types() -> Vec<Type>;
}

/// A typed statement's SQL, rendered for one store or one partition.
pub struct Rendered<S> {
    sql: SqlStr,
    query: PhantomData<fn() -> S>,
}

impl<S: Query> Rendered<S> {
    /// Binds `params`; run the result with one of [`Bound`]'s methods.
    pub fn bind<'q>(&'q self, params: S::Params<'q>) -> Bound<'q, S> {
        Bound {
            arguments: params.bind(sqlx::query(self.sql.clone())),
            query: PhantomData,
        }
    }

    /// The SQL of `V`, a variant of `S`, with `values` filled in.
    pub fn variant<V: Variant<S>>(values: &template::Values) -> Self {
        Self::new(V::STATEMENT.render(values))
    }

    /// The SQL of `S` composed in code from files of its registry entry, each rendered with its own values and
    /// joined by line breaks as [`template::Statement::render`] joins files: for a statement whose fragments
    /// repeat, such as one part per store. The registry entry holds one composition, which the registry test
    /// checks; the caller composes the same shape from the same files.
    pub fn compose<'a>(
        parts: impl IntoIterator<Item = (&'a template::File, template::Values)>,
    ) -> Self {
        let mut sql = String::new();
        for (i, (file, values)) in parts.into_iter().enumerate() {
            debug_assert!(
                S::STATEMENT.files.iter().any(|f| f.path == file.path),
                "{} is not a file of {}",
                file.path,
                S::STATEMENT.name
            );
            if i > 0 {
                sql.push('\n');
            }
            sql.push_str(&file.render(&values));
        }

        Self::new(sql)
    }

    /// The SQL.
    pub fn as_str(&self) -> &str {
        self.sql.as_str()
    }

    fn new(sql: impl Into<Cow<'static, str>>) -> Self {
        // The text is the registry's, with only validated placeholder values filled in. Shared,
        // so binding it does not copy it.
        let sql = match sql.into() {
            Cow::Borrowed(sql) => SqlStr::from_static(sql),
            Cow::Owned(sql) => AssertSqlSafe(Arc::<str>::from(sql)).into_sql_str(),
        };

        Self {
            sql,
            query: PhantomData,
        }
    }
}

impl<S> Clone for Rendered<S> {
    fn clone(&self) -> Self {
        Self {
            sql: self.sql.clone(),
            query: PhantomData,
        }
    }
}

impl<S> fmt::Debug for Rendered<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Rendered").field(&self.sql.as_str()).finish()
    }
}

/// A typed statement with its parameters bound.
#[must_use = "a bound query does nothing until it runs"]
pub struct Bound<'q, S> {
    arguments: Arguments<'q>,
    query: PhantomData<fn() -> S>,
}

impl<'q, S: Query> Bound<'q, S> {
    /// Runs the statement and returns how many rows it affected.
    pub async fn execute<'c, E>(self, executor: E) -> Result<PgQueryResult, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        executor.execute(self.arguments).await
    }

    /// Runs the statement and returns its first row, if any, to be decoded with [`Fetched::row`].
    pub async fn fetch_optional_row<'c, E>(
        self,
        executor: E,
    ) -> Result<Option<Fetched<S>>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let row = executor.fetch_optional(self.arguments).await?;

        Ok(row.map(Fetched::new))
    }

    /// Runs the statement and returns its rows, to be decoded with [`Fetched::row`].
    pub async fn fetch_all_rows<'c, E>(self, executor: E) -> Result<Vec<Fetched<S>>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let rows = executor.fetch_all(self.arguments).await?;

        Ok(rows.into_iter().map(Fetched::new).collect())
    }
}

impl<'q, S: Owned> Bound<'q, S> {
    /// Runs the statement and returns its first row; fails with [`sqlx::Error::RowNotFound`] if there is
    /// none.
    pub async fn fetch_one<'c, E>(self, executor: E) -> Result<S::Owned, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let row = executor.fetch_one(self.arguments).await?;

        S::Owned::decode(&row)
    }

    /// Runs the statement and returns its first row, if any.
    pub async fn fetch_optional<'c, E>(self, executor: E) -> Result<Option<S::Owned>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let row = executor.fetch_optional(self.arguments).await?;

        row.as_ref().map(S::Owned::decode).transpose()
    }

    /// Runs the statement and returns its rows.
    pub async fn fetch_all<'c, E>(self, executor: E) -> Result<Vec<S::Owned>, sqlx::Error>
    where
        E: Executor<'c, Database = Postgres>,
    {
        let rows = executor.fetch_all(self.arguments).await?;

        rows.iter().map(S::Owned::decode).collect()
    }
}

/// A row a typed statement returned, not decoded yet.
pub struct Fetched<S> {
    row: PgRow,
    query: PhantomData<fn() -> S>,
}

impl<S: Query> Fetched<S> {
    /// Decodes the row; the result may borrow from it.
    pub fn row(&self) -> Result<S::Row<'_>, sqlx::Error> {
        S::Row::decode(&self.row)
    }

    fn new(row: PgRow) -> Self {
        Self {
            row,
            query: PhantomData,
        }
    }
}

/// A typed statement as the registry test checks it: its entry and the Rust types of its parameters and
/// columns.
#[derive(Clone, Copy)]
pub struct Shape {
    /// The registry entry.
    pub statement: &'static template::Statement,
    /// The Rust type of each parameter.
    pub params: fn() -> Vec<Type>,
    /// The Rust type of each column.
    pub columns: fn() -> Vec<Type>,
}

impl Shape {
    /// The shape of `S`.
    pub const fn of<S: Query>() -> Self {
        Self {
            statement: S::STATEMENT,
            params: <S::Params<'static> as Params<'static>>::types,
            columns: <S::Row<'static> as Decode<'static>>::types,
        }
    }
}

impl fmt::Debug for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shape")
            .field("statement", &self.statement.name)
            .finish_non_exhaustive()
    }
}

/// A parameter's or column's Rust type, as the registry test sees it.
#[derive(Clone)]
pub struct Type {
    /// The Rust type's name.
    pub rust: &'static str,
    /// The PostgreSQL type sqlx sends for it (`sqlx::Type::type_info`).
    pub info: PgTypeInfo,
    /// The type [`Value`] gives it, which the registry entry holds; the registry test checks it is `info`.
    pub registered: template::PgType,
    /// Whether sqlx decodes it from a PostgreSQL type (`sqlx::Type::compatible`).
    pub compatible: fn(&PgTypeInfo) -> bool,
    /// Whether it holds NULL.
    pub null: Null,
}

impl Type {
    /// The type of `T`.
    pub fn of<T: Value + sqlx::Type<Postgres> + ?Sized>() -> Self {
        Self {
            rust: std::any::type_name::<T>(),
            info: T::type_info(),
            registered: T::TYPE,
            compatible: T::compatible,
            null: T::NULL,
        }
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Type")
            .field("rust", &self.rust)
            .field("info", &self.info)
            .field("null", &self.null)
            .finish_non_exhaustive()
    }
}

/// Whether a Rust type holds NULL: `Option<T>` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Null {
    /// `Option<T>`.
    Nullable,
    /// Any other type.
    NotNull,
}

/// A Rust type a typed statement binds or reads, with the PostgreSQL type sqlx gives it, in a form a
/// registry constant can hold. The registry test checks [`Value::TYPE`] against `sqlx::Type::type_info`.
pub trait Value {
    /// The PostgreSQL type.
    const TYPE: template::PgType;
    /// Whether the type holds NULL.
    const NULL: Null = Null::NotNull;
}

/// A Rust type that can be an array element, with the PostgreSQL type of the array.
pub trait Element {
    /// The PostgreSQL type of an array of it.
    const ARRAY: template::PgType;
}

macro_rules! value {
    ($($rust:ty => $pg:ident $(, $array:ident)?;)*) => {
        $(
            impl Value for $rust {
                const TYPE: template::PgType = template::PgType::$pg;
            }
            $(
                impl Element for $rust {
                    const ARRAY: template::PgType = template::PgType::$array;
                }
            )?
        )*
    };
}

value! {
    bool => BOOL;
    i16 => INT2, INT2_ARRAY;
    i32 => INT4, INT4_ARRAY;
    i64 => INT8, INT8_ARRAY;
    f64 => FLOAT8;
    str => TEXT;
    String => TEXT, TEXT_ARRAY;
    [u8] => BYTEA;
    Vec<u8> => BYTEA, BYTEA_ARRAY;
    Uuid => UUID, UUID_ARRAY;
    PgInterval => INTERVAL;
    () => VOID;
}

impl Element for &str {
    const ARRAY: template::PgType = template::PgType::TEXT_ARRAY;
}

impl Element for &[u8] {
    const ARRAY: template::PgType = template::PgType::BYTEA_ARRAY;
}

impl<T: Value + ?Sized> Value for &T {
    const TYPE: template::PgType = T::TYPE;
    const NULL: Null = T::NULL;
}

impl<T: Value> Value for Option<T> {
    const TYPE: template::PgType = T::TYPE;
    const NULL: Null = Null::Nullable;
}

impl<T: Element> Element for Option<T> {
    const ARRAY: template::PgType = T::ARRAY;
}

impl<T: Element> Value for [T] {
    const TYPE: template::PgType = T::ARRAY;
}

impl<T: Element> Value for Vec<T> {
    const TYPE: template::PgType = T::ARRAY;
}

impl Value for Oid {
    const TYPE: template::PgType = template::PgType::named("OID");
}

impl Value for PgLsn {
    const TYPE: template::PgType = template::PgType::named(PgLsn::TYPE_NAME);
}

/// A `pg_lsn`: a position in the WAL, as a byte offset. sqlx has no type for it. It is written `X/Y`, the high
/// and low 32 bits in hexadecimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgLsn(u64);

impl PgLsn {
    const TYPE_NAME: &'static str = "pg_lsn";

    /// The position `offset` bytes into the WAL.
    pub const fn new(offset: u64) -> Self {
        Self(offset)
    }

    /// The byte offset.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl std::str::FromStr for PgLsn {
    type Err = InvalidPgLsn;

    /// Parses `X/Y`, as PostgreSQL writes a `pg_lsn`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidPgLsn(text.to_owned());
        let (high, low) = text.split_once('/').ok_or_else(invalid)?;
        let high = u32::from_str_radix(high, 16).map_err(|_| invalid())?;
        let low = u32::from_str_radix(low, 16).map_err(|_| invalid())?;

        Ok(Self((u64::from(high) << 32) | u64::from(low)))
    }
}

/// A text that is not a `pg_lsn` (`X/Y` in hexadecimal).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a pg_lsn: {0:?}")]
pub struct InvalidPgLsn(String);

impl sqlx::Type<Postgres> for PgLsn {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name(Self::TYPE_NAME)
    }
}

impl sqlx::Encode<'_, Postgres> for PgLsn {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        // The binary form of a pg_lsn is its offset as a big-endian 64-bit integer (pg_lsn_send).
        buf.extend_from_slice(&self.0.to_be_bytes());

        Ok(sqlx::encode::IsNull::No)
    }
}

/// Declares a typed statement: the registry constant `$CONST` and a module `$module` holding the marker type
/// `Query` and the `Params` and `Row` structs. Not a stable API.
///
/// ```text
/// typed_query! {
///     /// Locks a stream's row.
///     pub LOCK_STREAM, mod lock_stream {
///         name: "append/lock_stream",
///         files: [sql_file!("append/lock_stream.sql")],
///         params { stream_id: Uuid },
///         row { last_commit_id: Uuid },
///     }
/// }
/// ```
///
/// `params` and `row` take a lifetime when a field borrows (`&'q str`, `&'r [u8]`). A column whose name is
/// not an identifier is read as `field: Type as "?column?"`. A column whose name another column of the row
/// shares (`count(*), count(*) FILTER (..)`) is read by its position, from 0, as
/// `field: Type as "count" at 1`; the name is still checked when the row is read. It does not compile when a
/// column read by name shares its name with another, or a column read `at N` is not the row's field at
/// position `N`. The field types resolve in the module that invokes the macro.
#[macro_export]
#[doc(hidden)]
macro_rules! typed_query {
    (
        $(#[$doc:meta])*
        $vis:vis $constant:ident, mod $module:ident {
            name: $name:literal,
            files: [$($file:expr),+ $(,)?],
            params $(<$plt:lifetime>)? { $($param:ident: $param_ty:ty),* $(,)? },
            row $(<$rlt:lifetime>)? { $($column:ident: $column_ty:ty $(as $column_name:literal $(at $column_index:literal)?)?),* $(,)? } $(,)?
        }
    ) => {
        $(#[$doc])*
        $vis const $constant: $crate::sql::template::Statement = $crate::sql::template::Statement {
            name: $name,
            files: &[$($file),+],
            kind: $crate::sql::template::Kind::Query,
            params: <<$module::Query as $crate::sql::typed::Query>::Params<'static>
                as $crate::sql::typed::Params<'static>>::TYPES,
            columns: <<$module::Query as $crate::sql::typed::Query>::Row<'static>
                as $crate::sql::typed::Decode<'static>>::COLUMNS,
        };

        $(#[$doc])*
        $vis mod $module {
            #[allow(unused_imports)]
            use super::*;

            #[doc = concat!("The typed statement `", $name, "`.")]
            #[derive(Debug, Clone, Copy)]
            pub struct Query;

            #[doc = concat!("The parameters of `", $name, "`, bound as `$1`, `$2`, ... in order.")]
            #[derive(Debug)]
            pub struct Params $(<$plt>)? {
                $(
                    #[doc = concat!("`", stringify!($param), "`.")]
                    pub $param: $param_ty,
                )*
            }

            #[doc = concat!("A row of `", $name, "`.")]
            #[derive(Debug)]
            pub struct Row $(<$rlt>)? {
                $(
                    #[doc = concat!("The column `", $crate::typed_query!(@column $column $($column_name)?), "`.")]
                    pub $column: $column_ty,
                )*
            }

            impl $crate::sql::typed::Query for Query {
                const STATEMENT: &'static $crate::sql::template::Statement = &super::$constant;
                type Params<'__q> = $crate::typed_query!(@alias Params [$($plt)?] '__q);
                type Row<'__r> = $crate::typed_query!(@alias Row [$($rlt)?] '__r);
            }

            $crate::typed_query!(@owned [$($rlt)?]);

            $crate::typed_query!(@params [$($plt)?] { $($param: $param_ty),* });

            $crate::typed_query!(@row [$($rlt)?] { $($column: $column_ty $(as $column_name $(at $column_index)?)?),* });
        }
    };

    (
        $(#[$doc:meta])*
        $vis:vis $constant:ident, mod $module:ident {
            name: $name:literal,
            files: [$($file:expr),+ $(,)?],
            like $original:ident $(,)?
        }
    ) => {
        $(#[$doc])*
        $vis const $constant: $crate::sql::template::Statement = $crate::sql::template::Statement {
            name: $name,
            files: &[$($file),+],
            kind: $crate::sql::template::Kind::Query,
            params: <<$module::Query as $crate::sql::typed::Query>::Params<'static>
                as $crate::sql::typed::Params<'static>>::TYPES,
            columns: <<$module::Query as $crate::sql::typed::Query>::Row<'static>
                as $crate::sql::typed::Decode<'static>>::COLUMNS,
        };

        $(#[$doc])*
        $vis mod $module {
            #[doc = concat!("The typed statement `", $name, "`, a variant of `", stringify!($original), "`.")]
            #[derive(Debug, Clone, Copy)]
            pub struct Query;

            impl $crate::sql::typed::Query for Query {
                const STATEMENT: &'static $crate::sql::template::Statement = &super::$constant;
                type Params<'__q> =
                    <super::$original::Query as $crate::sql::typed::Query>::Params<'__q>;
                type Row<'__r> = <super::$original::Query as $crate::sql::typed::Query>::Row<'__r>;
            }

            impl $crate::sql::typed::Variant<super::$original::Query> for Query {}
        }
    };

    (@column $column:ident) => { stringify!($column) };
    (@column $column:ident $name:literal) => { $name };

    (@read $row:ident, $column:ident) => {
        $crate::sql::typed::column($row, stringify!($column))
    };
    (@read $row:ident, $column:ident $name:literal) => {
        $crate::sql::typed::column($row, $name)
    };
    (@read $row:ident, $column:ident $name:literal $index:literal) => {
        $crate::sql::typed::column_at($row, $name, $index)
    };

    (@alias $ty:ident [] $lt:lifetime) => { $ty };
    (@alias $ty:ident [$own:lifetime] $lt:lifetime) => { $ty<$lt> };

    (@owned []) => {
        impl $crate::sql::typed::Owned for Query {
            type Owned = Row;
        }
    };
    (@owned [$lt:lifetime]) => {};

    (@params [] { $($param:ident: $param_ty:ty),* }) => {
        $crate::typed_query!(@params_impl '__q, Params { $($param: $param_ty),* });
    };
    (@params [$lt:lifetime] { $($param:ident: $param_ty:ty),* }) => {
        $crate::typed_query!(@params_impl $lt, Params<$lt> { $($param: $param_ty),* });
    };
    (@params_impl $lt:lifetime, $self:ty { $($param:ident: $param_ty:ty),* }) => {
        impl<$lt> $crate::sql::typed::Params<$lt> for $self {
            const TYPES: &'static [$crate::sql::template::PgType] =
                &[$(<$param_ty as $crate::sql::typed::Value>::TYPE),*];

            #[allow(unused_mut)]
            fn bind(
                self,
                mut query: $crate::sql::typed::Arguments<$lt>,
            ) -> $crate::sql::typed::Arguments<$lt> {
                $(query = query.bind(self.$param);)*

                query
            }

            fn types() -> ::std::vec::Vec<$crate::sql::typed::Type> {
                ::std::vec![$($crate::sql::typed::Type::of::<$param_ty>()),*]
            }
        }
    };

    (@row [] { $($column:ident: $column_ty:ty $(as $column_name:literal $(at $column_index:literal)?)?),* }) => {
        $crate::typed_query!(@row_impl '__r, Row { $($column: $column_ty $(as $column_name $(at $column_index)?)?),* });
    };
    (@row [$lt:lifetime] { $($column:ident: $column_ty:ty $(as $column_name:literal $(at $column_index:literal)?)?),* }) => {
        $crate::typed_query!(@row_impl $lt, Row<$lt> { $($column: $column_ty $(as $column_name $(at $column_index)?)?),* });
    };
    (@row_impl $lt:lifetime, $self:ty { $($column:ident: $column_ty:ty $(as $column_name:literal $(at $column_index:literal)?)?),* }) => {
        impl<$lt> $crate::sql::typed::Decode<$lt> for $self {
            const COLUMNS: &'static [$crate::sql::template::Column] = &[$(
                $crate::sql::template::Column::new(
                    $crate::typed_query!(@column $column $($column_name)?),
                    <$column_ty as $crate::sql::typed::Value>::TYPE,
                )
            ),*];

            #[allow(unused_variables)]
            fn decode(
                row: &$lt ::sqlx::postgres::PgRow,
            ) -> ::std::result::Result<Self, ::sqlx::Error> {
                ::std::result::Result::Ok(Self {
                    $($column: $crate::typed_query!(
                        @read row, $column $($column_name $($column_index)?)?
                    )?,)*
                })
            }

            fn types() -> ::std::vec::Vec<$crate::sql::typed::Type> {
                ::std::vec![$($crate::sql::typed::Type::of::<$column_ty>()),*]
            }
        }

        // A column read by name must be the only one of its name, and a column read by position must be read
        // into the field at that position.
        #[allow(dead_code, unused_mut, unused_variables, unused_assignments)]
        const _: () = {
            const NAMES: &[&str] = &[$($crate::typed_query!(@column $column $($column_name)?)),*];
            let mut field = 0usize;
            $(
                $crate::typed_query!(@check NAMES, field, $column $($column_name $($column_index)?)?);
                field += 1;
            )*
        };
    };

    (@check $names:ident, $field:ident, $column:ident) => {
        ::std::assert!(
            $crate::sql::typed::named_once($names, ::std::stringify!($column)),
            ::std::concat!("another column is named `", ::std::stringify!($column), "`: read each with `at`"),
        );
    };
    (@check $names:ident, $field:ident, $column:ident $name:literal) => {
        ::std::assert!(
            $crate::sql::typed::named_once($names, $name),
            ::std::concat!("another column is named `", $name, "`: read each with `at`"),
        );
    };
    (@check $names:ident, $field:ident, $column:ident $name:literal $index:literal) => {
        ::std::assert!(
            $field == $index,
            ::std::concat!(
                "the field `", ::std::stringify!($column), "` is read `at ", ::std::stringify!($index),
                "`, but is not at position ", ::std::stringify!($index), " of the row",
            ),
        );
    };
}

/// Whether `name` is in `names` exactly once; for [`typed_query!`](crate::typed_query), which reads a column by
/// name only when no other column shares it.
#[doc(hidden)]
pub const fn named_once(names: &[&str], name: &str) -> bool {
    let mut count = 0;
    let mut i = 0;
    while i < names.len() {
        if same(names[i].as_bytes(), name.as_bytes()) {
            count += 1;
        }
        i += 1;
    }

    count == 1
}

const fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }

    true
}

/// Reads the column `name` of `row`; for [`typed_query!`](crate::typed_query).
#[doc(hidden)]
pub fn column<'r, T>(row: &'r PgRow, name: &str) -> Result<T, sqlx::Error>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    sqlx::Row::try_get(row, name)
}

/// Reads the column at `index` of `row`, which must be named `name`; for [`typed_query!`](crate::typed_query),
/// for a column whose name another column shares.
#[doc(hidden)]
pub fn column_at<'r, T>(row: &'r PgRow, name: &str, index: usize) -> Result<T, sqlx::Error>
where
    T: sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    let named = sqlx::Row::columns(row)
        .get(index)
        .is_some_and(|column| sqlx::Column::name(column) == name);
    if !named {
        return Err(sqlx::Error::ColumnNotFound(format!("{name} at {index}")));
    }

    sqlx::Row::try_get(row, index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_lsn_reads_and_writes_as_postgres_does() {
        let lsn: PgLsn = "16/B374D848".parse().unwrap();

        assert_eq!(lsn.get(), 0x16_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!("0/0".parse::<PgLsn>().unwrap(), PgLsn::new(0));
        assert_eq!(PgLsn::new(u64::MAX).to_string(), "FFFFFFFF/FFFFFFFF");
        for invalid in ["", "16", "16/", "/1", "g/1", "1/100000000", "1/2/3"] {
            assert!(invalid.parse::<PgLsn>().is_err(), "{invalid:?}");
        }
    }
}
