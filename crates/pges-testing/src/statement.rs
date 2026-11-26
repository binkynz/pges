//! Checks of a crate's statement registry (`pges_postgres::sql`) against PostgreSQL 18.
//!
//! A crate's registry test renders every statement with [`template::Values`] for a test store, then:
//!
//! - [`check_files`]: the embedded files match `sql/` on disk, every file there is used, each starts with a
//!   header comment, statement names are unique, and no placeholder is left after rendering;
//! - [`check_typed`]: each query matches the Rust types of its parameters and columns;
//! - [`run_ddl`]: DDL renders without leftover placeholders and runs;
//! - [`check_no_inline_sql`]: the crate's source passes no SQL literal or `format!` SQL to sqlx;
//! - [`check_typed_calls`]: the crate's source binds and reads only through typed statements.

use std::{
    collections::{BTreeSet, HashSet},
    fmt::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use sqlx::{
    Column, Connection, Either, Executor, PgConnection, Statement, TypeInfo,
    postgres::{PgColumn, PgTypeInfo, types::Oid},
};

use pges_postgres::sql::{template, typed};

/// Checks a typed statement (`pges_postgres::sql::typed`) against PostgreSQL: its registry types are the
/// PostgreSQL types sqlx gives its Rust types, and both as PostgreSQL infers the parameters and as sqlx runs it
/// (every parameter given the type of its Rust value), each parameter's Rust type accepts the inferred type,
/// the columns have the registry's names, and each column's Rust type decodes the type PostgreSQL reports.
///
/// Nullability, where `describe` reports it: a column read as `Option<T>` must be nullable, and one read as
/// `T` must not be. Where PostgreSQL cannot tell (an expression, or a statement whose parameters had to be
/// given types), either is accepted. On a failure, the error lists what PostgreSQL reports next to the Rust
/// types.
pub async fn check_typed(
    conn: &mut PgConnection,
    shape: &typed::Shape,
    values: &template::Values,
) -> Result<()> {
    let statement = shape.statement;
    ensure!(
        statement.kind == template::Kind::Query,
        "{} is not a query",
        statement.name
    );
    let params = (shape.params)();
    let columns = (shape.columns)();

    let mut problems = Vec::new();
    for (what, ty) in params
        .iter()
        .map(|t| ("parameter", t))
        .chain(columns.iter().map(|t| ("column", t)))
    {
        if ty.registered.name() != ty.info.name() {
            problems.push(format!(
                "{what} {}: registered as {}, but sqlx sends it as {}",
                ty.rust,
                ty.registered,
                ty.info.name()
            ));
        }
    }

    let sql = rendered(statement, values)?;
    let inferred = infer(conn, statement, &sql, &params).await?;
    compare(
        &mut problems,
        "inferred",
        &inferred,
        &params,
        statement,
        &columns,
    );

    // sqlx caches a prepared statement by its SQL alone, with the types it was first prepared with.
    conn.clear_cached_statements().await?;
    let types: Vec<PgTypeInfo> = params.iter().map(|t| t.info.clone()).collect();
    match conn
        .prepare_with(template::text(sql.as_str()), &types)
        .await
    {
        Ok(prepared) => {
            let run = Inferred::of(statement, prepared.parameters(), prepared.columns(), |_| {
                None
            })?;
            compare(&mut problems, "as run", &run, &params, statement, &columns);
        }
        // Reported with the listing, which shows the types PostgreSQL expects.
        Err(error) => problems.push(format!(
            "as run: PostgreSQL rejects the Rust parameter types: {error}"
        )),
    }

    ensure!(
        problems.is_empty(),
        "{} does not match its Rust types:\n  {}\nPostgreSQL describes it as\n{}",
        statement.name,
        problems.join("\n  "),
        inferred.listing()
    );

    Ok(())
}

/// Checks that `statement` is DDL without parameters or columns, renders it and runs it.
pub async fn run_ddl(
    conn: &mut PgConnection,
    statement: &template::Statement,
    values: &template::Values,
) -> Result<()> {
    ensure!(
        statement.kind == template::Kind::Ddl,
        "{} is not DDL",
        statement.name
    );
    ensure!(
        statement.params.is_empty() && statement.columns.is_empty(),
        "{}: DDL takes no parameters and returns no rows",
        statement.name
    );

    let sql = rendered(statement, values)?;
    conn.execute(template::text(sql.as_str()))
        .await
        .with_context(|| format!("running {}:\n{sql}", statement.name))?;

    Ok(())
}

/// Checks a registry against the crate's `sql/` directory (`manifest_dir` is the crate's
/// `CARGO_MANIFEST_DIR`): every embedded file has the text of its file on disk, every `.sql` file there is
/// used, each starts with a `--` header comment, statement names are unique, rendering with `values` leaves no
/// placeholder, and a statement of one file without placeholders renders to [`template::Statement::sql`].
pub fn check_files(
    manifest_dir: &Path,
    registry: &[&template::Statement],
    values: &template::Values,
) -> Result<()> {
    let root = manifest_dir.join("sql");
    let mut names = HashSet::new();
    let mut used = BTreeSet::new();

    for statement in registry {
        ensure!(
            names.insert(statement.name),
            "{} is registered twice",
            statement.name
        );
        ensure!(
            !statement.files.is_empty(),
            "{} has no file",
            statement.name
        );
        for file in statement.files {
            let on_disk = std::fs::read_to_string(root.join(file.path))
                .with_context(|| format!("{}: reading sql/{}", statement.name, file.path))?;
            ensure!(
                on_disk == file.text,
                "{}: sql/{} differs from the embedded text",
                statement.name,
                file.path
            );
            ensure!(
                file.text.starts_with("-- "),
                "sql/{} must start with a header comment",
                file.path
            );
            used.insert(PathBuf::from(file.path));
        }

        rendered(statement, values)?;
        let unfilled = statement
            .files
            .iter()
            .all(|file| template::placeholders(file).is_empty());
        if statement.files.len() == 1 && unfilled {
            ensure!(
                statement.sql() == statement.render(&template::Values::new()),
                "{}: sql() differs from render()",
                statement.name
            );
        }
    }

    let on_disk = sql_files(&root, &root)?;
    let unused: Vec<String> = on_disk
        .difference(&used)
        .map(|p| p.display().to_string())
        .collect();
    ensure!(
        unused.is_empty(),
        "files under sql/ no statement uses: {unused:?}"
    );

    Ok(())
}

/// Checks that the crate's non-test source (`src`, up to each file's first `#[cfg(test)]`) passes sqlx no SQL
/// of its own: no string literal or `format!` as the statement of `sqlx::query*`, `raw_sql`, or an
/// `Executor` method (wrapped in `AssertSqlSafe` or `template::text` or not), no `format!` whose text holds SQL, and no string literal that starts with SQL (outside
/// `//` comments). All SQL comes from the registry.
pub fn check_no_inline_sql(src: &Path) -> Result<()> {
    let mut violations = String::new();
    for path in rust_files(src)? {
        let text = std::fs::read_to_string(&path)?;
        let code = text.split("#[cfg(test)]").next().unwrap_or_default();
        for (line, found) in inline_sql(code) {
            let _ = writeln!(violations, "{}:{line}: {found}", path.display());
        }
    }
    ensure!(
        violations.is_empty(),
        "SQL outside the registry:\n{violations}"
    );

    Ok(())
}

/// Checks that the crate's non-test source (`src`, up to each file's first `#[cfg(test)]`) binds parameters and
/// reads columns only through typed statements (`pges_postgres::sql::typed`): no `sqlx::query*`, `sqlx::Row`,
/// `query_as`, `query_scalar` or `try_get` outside `//` comments, and no `use sqlx::` of `query`, `query_with`
/// or `Row`. `except` names files below `src` that may, such as the
/// one implementing the typed statements. DDL and other statements without parameters may still run as text
/// through an `Executor`.
pub fn check_typed_calls(src: &Path, except: &[&str]) -> Result<()> {
    let mut violations = String::new();
    for path in rust_files(src)? {
        if except.iter().any(|e| path == src.join(e)) {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let code = text.split("#[cfg(test)]").next().unwrap_or_default();
        for (line, found) in untyped_calls(code) {
            let _ = writeln!(violations, "{}:{line}: {found}", path.display());
        }
    }
    ensure!(
        violations.is_empty(),
        "binds or reads outside the typed statements:\n{violations}"
    );

    Ok(())
}

/// Calls that bind parameters or read columns without a typed statement. `sqlx::Row` is the trait behind
/// `get` and `try_get`.
const UNTYPED_CALLS: &[&str] = &[
    "sqlx::query",
    "sqlx::Row",
    "query_as",
    "query_scalar",
    "try_get",
];

/// Names a `use sqlx::{..}` may not import, since `query(sql)` or `row.get(..)` would bypass the typed
/// statements once imported.
const UNTYPED_IMPORTS: &[&str] = &["query", "query_with", "Row"];

/// The untyped calls in `code` outside `//` comments, with their line (once per line).
fn untyped_calls(code: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let mut push = |at: usize| {
        let line_start = code[..at].rfind('\n').map_or(0, |i| i + 1);
        let line = &code[line_start..];
        if !line.trim_start().starts_with("//") {
            let line_number = code[..at].matches('\n').count() + 1;
            found.push((line_number, first_line(line).trim().to_owned()));
        }
    };
    for call in UNTYPED_CALLS {
        for (at, _) in code.match_indices(call) {
            push(at);
        }
    }
    for (at, _) in code.match_indices("use sqlx::") {
        let item = &code[at..code[at..].find(';').map_or(code.len(), |end| at + end)];
        let imports_untyped = item
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .any(|name| UNTYPED_IMPORTS.contains(&name));
        if imports_untyped {
            push(at);
        }
    }
    found.sort();
    found.dedup_by_key(|(line, _)| *line);

    found
}

const CALLS: &[&str] = &[
    "sqlx::query(",
    "sqlx::query_as(",
    "sqlx::query_scalar(",
    "sqlx::query_as::<",
    "sqlx::query_scalar::<",
    "sqlx::raw_sql(",
    ".execute(",
    ".execute_many(",
    ".fetch(",
    ".fetch_all(",
    ".fetch_one(",
    ".fetch_optional(",
    ".fetch_many(",
    ".prepare(",
    ".describe(",
];

/// Words that start SQL. A text holds SQL where one of them is followed by whitespace or `(`, so these
/// literals do not count themselves.
const SQL_WORDS: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "ALTER",
    "CREATE",
    "DROP",
    "VACUUM",
    "ANALYZE",
    "CHECKPOINT",
    "SET",
    "RESET",
    "WITH",
    "TRUNCATE",
    "PREPARE",
    "EXECUTE",
    "EXPLAIN",
    "DEALLOCATE",
];

/// Calls that take a literal or `format!` as SQL, `format!` texts that hold SQL, and string literals that
/// start with SQL, with their line (once per line). Code in `//` comments, doc comments included, is skipped.
fn inline_sql(code: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let line_of = |at: usize| code[..at].matches('\n').count() + 1;
    let in_comment = |at: usize| {
        let line_start = code[..at].rfind('\n').map_or(0, |i| i + 1);
        code[line_start..at].trim_start().starts_with("//")
    };

    for call in CALLS {
        for (at, _) in code.match_indices(call) {
            if in_comment(at) {
                continue;
            }
            let mut rest = &code[at + call.len()..];
            if call.ends_with('<') {
                let Some(open) = rest.find(">(") else {
                    continue;
                };
                rest = &rest[open + 2..];
            }
            let arg = unwrap_text(rest);
            if arg.starts_with('"')
                || arg.starts_with("r\"")
                || arg.starts_with("r#")
                || arg.starts_with("format!")
            {
                found.push((line_of(at), format!("{call}{}", first_line(arg))));
            }
        }
    }
    for (at, _) in code.match_indices("format!(") {
        if in_comment(at) {
            continue;
        }
        let literal = code[at + "format!(".len()..].trim_start();
        let Some(body) = literal.strip_prefix('"') else {
            continue;
        };
        let end = body.find('"').unwrap_or(body.len());
        let text = &body[..end];
        if contains_sql(text) {
            found.push((line_of(at), format!("format!(\"{}", first_line(text))));
        }
    }
    for (at, _) in code.match_indices('"') {
        let text = &code[at + 1..];
        if !in_comment(at) && starts_with_sql(text) {
            found.push((line_of(at), format!("\"{}", first_line(text))));
        }
    }
    found.sort_by_key(|(line, _)| *line);
    found.dedup_by_key(|(line, _)| *line);

    found
}

/// Wrappers that pass a text on to sqlx as SQL: sqlx 0.9's assertion that a non-`'static` text is safe, and the
/// registry's conversion of rendered SQL.
const TEXT_WRAPPERS: &[&str] = &[
    "sqlx::AssertSqlSafe(",
    "AssertSqlSafe(",
    "template::text(",
    "text(",
];

/// A call's argument without a leading `&` or [`TEXT_WRAPPERS`], so a literal inside a wrapper is found.
fn unwrap_text(arg: &str) -> &str {
    let mut arg = arg.trim_start().trim_start_matches('&');
    while let Some(inner) = TEXT_WRAPPERS.iter().find_map(|w| arg.strip_prefix(w)) {
        arg = inner.trim_start().trim_start_matches('&');
    }

    arg
}

/// Whether `text` starts with a word of [`SQL_WORDS`] followed by whitespace or `(`.
fn starts_with_sql(text: &str) -> bool {
    SQL_WORDS.iter().any(|word| {
        text.strip_prefix(word)
            .and_then(|rest| rest.chars().next())
            .is_some_and(|next| next.is_ascii_whitespace() || next == '(')
    })
}

/// Whether a word of [`SQL_WORDS`] followed by whitespace or `(` appears anywhere in `text`.
fn contains_sql(text: &str) -> bool {
    SQL_WORDS.iter().any(|word| {
        text.match_indices(word)
            .any(|(at, _)| starts_with_sql(&text[at..]))
    })
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

/// What PostgreSQL reports for a statement.
struct Inferred {
    params: Vec<PgTypeInfo>,
    columns: Vec<InferredColumn>,
}

struct InferredColumn {
    name: String,
    ty: PgTypeInfo,
    /// `None` where PostgreSQL cannot tell.
    nullable: Option<bool>,
}

impl Inferred {
    fn of(
        statement: &template::Statement,
        parameters: Option<Either<&[PgTypeInfo], usize>>,
        columns: &[PgColumn],
        nullable: impl Fn(usize) -> Option<bool>,
    ) -> Result<Self> {
        let params = match parameters {
            Some(Either::Left(types)) => types.to_vec(),
            Some(Either::Right(count)) => {
                bail!("{}: {count} parameters of unknown type", statement.name)
            }
            None => Vec::new(),
        };
        let columns = columns
            .iter()
            .enumerate()
            .map(|(at, c)| InferredColumn {
                name: c.name().to_owned(),
                ty: c.type_info().clone(),
                nullable: nullable(at),
            })
            .collect();

        Ok(Self { params, columns })
    }

    /// One line per parameter and column, for an error message.
    fn listing(&self) -> String {
        let mut listing = String::new();
        for (at, ty) in self.params.iter().enumerate() {
            let _ = writeln!(listing, "  ${}: {}", at + 1, ty.name());
        }
        for column in &self.columns {
            let nullable = match column.nullable {
                Some(true) => "nullable",
                Some(false) => "not null",
                None => "nullability unknown",
            };
            let _ = writeln!(
                listing,
                "  column {:?}: {}, {nullable}",
                column.name,
                column.ty.name()
            );
        }

        listing
    }
}

/// Asks PostgreSQL to describe `sql`. When it cannot infer a parameter's type (`$1 >= $8` where both are
/// otherwise untyped), the statement is prepared again with that one parameter given its Rust type's, as sqlx
/// does at run time with the types of the bound values, and so on until PostgreSQL infers the rest.
async fn infer(
    conn: &mut PgConnection,
    statement: &template::Statement,
    sql: &str,
    given: &[typed::Type],
) -> Result<Inferred> {
    let context = || format!("describing {}:\n{sql}", statement.name);

    let mut error = match conn.describe(template::text(sql)).await {
        Ok(described) => {
            return Inferred::of(
                statement,
                described.parameters(),
                described.columns(),
                |at| described.nullable(at),
            );
        }
        Err(error) => error,
    };
    let count = given.len();
    // Oid 0 leaves a parameter's type to PostgreSQL.
    let mut types = vec![PgTypeInfo::with_oid(Oid(0)); count];
    let mut typed = vec![false; count];
    while is_indeterminate(&error) {
        let Some(at) = parameter_named(&error)
            .filter(|at| typed.get(*at) == Some(&false))
            .or_else(|| typed.iter().position(|g| !g))
        else {
            break;
        };
        types[at] = given[at].info.clone();
        typed[at] = true;
        match conn.prepare_with(template::text(sql), &types).await {
            Ok(prepared) => {
                return Inferred::of(statement, prepared.parameters(), prepared.columns(), |_| {
                    None
                });
            }
            Err(next) => error = next,
        }
    }

    Err(anyhow::Error::new(error).context(context()))
}

/// Compares what PostgreSQL reports (`how`: inferred, or as run) with a typed statement's Rust types.
fn compare(
    problems: &mut Vec<String>,
    how: &str,
    reported: &Inferred,
    params: &[typed::Type],
    statement: &template::Statement,
    columns: &[typed::Type],
) {
    if reported.params.len() != params.len() {
        problems.push(format!(
            "{how}: {} parameters, but {} Rust types",
            reported.params.len(),
            params.len()
        ));
    }
    for (at, (pg, rust)) in reported.params.iter().zip(params).enumerate() {
        if !(rust.compatible)(pg) {
            problems.push(format!(
                "{how}: ${} is {}, which {} does not accept",
                at + 1,
                pg.name(),
                rust.rust
            ));
        }
    }

    if reported.columns.len() != columns.len() {
        problems.push(format!(
            "{how}: {} columns, but {} Rust types",
            reported.columns.len(),
            columns.len()
        ));
    }
    for ((pg, rust), registered) in reported.columns.iter().zip(columns).zip(statement.columns) {
        if pg.name != registered.name {
            problems.push(format!(
                "{how}: column {:?} is registered as {:?}",
                pg.name, registered.name
            ));
        }
        if !(rust.compatible)(&pg.ty) {
            problems.push(format!(
                "{how}: column {:?} is {}, which {} does not decode",
                pg.name,
                pg.ty.name(),
                rust.rust
            ));
        }
        match (rust.null, pg.nullable) {
            (typed::Null::NotNull, Some(true)) => problems.push(format!(
                "{how}: column {:?} is nullable, but read as {}",
                pg.name, rust.rust
            )),
            (typed::Null::Nullable, Some(false)) => problems.push(format!(
                "{how}: column {:?} is never NULL, but read as {}",
                pg.name, rust.rust
            )),
            _ => {}
        }
    }
}

/// Whether PostgreSQL could not infer a parameter's type: 42P18 (indeterminate datatype) or 42P08
/// (ambiguous parameter), or 42804 for inconsistent types deduced for one parameter.
fn is_indeterminate(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| matches!(code.as_ref(), "42P18" | "42P08" | "42804"))
}

/// The index of the parameter an error names (`... parameter $3`), if any.
fn parameter_named(error: &sqlx::Error) -> Option<usize> {
    let message = error.as_database_error()?.message();
    let (_, after) = message.split_once("parameter $")?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();

    digits.parse::<usize>().ok()?.checked_sub(1)
}

fn rendered(statement: &template::Statement, values: &template::Values) -> Result<String> {
    let sql = statement.render(values);
    let leftover = template::leftover_placeholders(&sql);
    ensure!(
        leftover.is_empty(),
        "{}: placeholders without a value, or unknown: {leftover:?}",
        statement.name
    );

    Ok(sql)
}

fn sql_files(root: &Path, dir: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(sql_files(root, &path)?);
        } else if path.extension().is_some_and(|e| e == "sql") {
            files.insert(path.strip_prefix(root)?.to_path_buf());
        }
    }

    Ok(files)
}

fn rust_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(rust_files(&path)?);
        } else if path.extension().is_some_and(|e| e == "rs") {
            files.push(path);
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_untyped_binds_and_reads() {
        let code = r#"
            let row = sqlx::query(sql).bind(id).fetch_one(&mut conn).await?;
            let id: i32 = row.try_get("id")?;
            // sqlx::query_as(sql) in a comment.
            sql.append.bind(statement::append::Params { id }).fetch_one(&mut conn).await?;
            let n: i64 = sqlx::query_scalar::<_, i64>(sql).fetch_one(&mut conn).await?;
            conn.execute(statement::MAINTAIN_SESSION.sql()).await?;
            use sqlx::{Executor, Row, postgres::PgRow};
            use sqlx::query;
            use sqlx::{Executor, PgConnection, postgres::PgRow};
            let id: i32 = sqlx::Row::get(&row, "id");
        "#;

        let found: Vec<usize> = untyped_calls(code)
            .into_iter()
            .map(|(line, _)| line)
            .collect();

        assert_eq!(found, [2, 3, 6, 8, 9, 11]);
    }

    #[test]
    fn finds_sql_passed_inline() {
        let code = r#"
            sqlx::query("SELECT 1").execute(&mut conn);
            sqlx::query_as::<_, (i32,)>(&format!("SELECT {x}"));
            conn.execute("SET x = 1");
            let s = format!("DROP TABLE {t}");
            sqlx::query(statement::A.sql());
            conn.execute(sql.as_str());
            let name = format!("{store}.log");
            const LIST: &str = "SELECT name FROM t";
            /// Runs "SELECT 1".
            //! sqlx::query("UPDATE t SET n = 1").execute(&mut *tx);
            const WORDS: &[&str] = &["SELECT", "SET"];
            pool.execute("CHECKPOINT");
            let s = format!("EXPLAIN (ANALYZE) EXECUTE {name}");
            pool.execute(sqlx::AssertSqlSafe("CHECKPOINT"));
            conn.execute(template::text(&format!("{x}")));
            conn.execute(template::text(sql.as_str()));
        "#;

        let found: Vec<usize> = inline_sql(code).into_iter().map(|(line, _)| line).collect();

        assert_eq!(
            found.iter().copied().collect::<BTreeSet<_>>(),
            [2, 3, 4, 5, 9, 13, 14, 15, 16].into()
        );
    }
}
