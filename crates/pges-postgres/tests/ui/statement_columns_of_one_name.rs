// Columns that share a name (`count(*), count(*) FILTER (..)`) are read by position: one read by name, or read
// into a field at another position, does not compile.

use pges_postgres::sql::template;

const COUNTS: template::File = template::File {
    path: "counts.sql",
    text: "-- Two counts.\nSELECT count(*), count(*) FILTER (WHERE true) FROM pg_class",
};

pges_postgres::typed_query! {
    /// Read by name.
    pub BY_NAME, mod by_name {
        name: "counts/by_name",
        files: [COUNTS],
        params {},
        row { all: i64 as "count", some: i64 as "count" at 1 },
    }
}

pges_postgres::typed_query! {
    /// Read swapped.
    pub SWAPPED, mod swapped {
        name: "counts/swapped",
        files: [COUNTS],
        params {},
        row { all: i64 as "count" at 1, some: i64 as "count" at 0 },
    }
}

fn main() {}
