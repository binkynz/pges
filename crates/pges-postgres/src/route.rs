//! Which hash partition of `streams` holds a stream, computed the way PostgreSQL routes a row, so an append
//! can name its stream's partition and no plan has to prune the others.
//!
//! PostgreSQL hashes a `uuid` partition key with `uuid_hash_extended` (Bob Jenkins' lookup3 over the 16
//! bytes, seeded with `HASH_PARTITION_SEED`), combines the key hashes with `hash_combine64` starting from 0,
//! and takes the result modulo the partitions' modulus (`compute_partition_hash_value` and
//! `get_partition_for_tuple` in `partbounds.c`). The hash reads the bytes as native 32-bit words, so this
//! matches a little-endian server only; a store checks it against the server when it opens.

use uuid::Uuid;

use crate::sql::{statement, template};

/// The hash partitions of a store's `streams`, by remainder, when the store can route appends to them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Leaves {
    modulus: u16,
    names: Vec<template::Ident>,
}

impl Leaves {
    /// The partitions `append/stream_leaves.sql` listed for [`probes`], or `None` unless they are a plain
    /// hash partitioning this module computes exactly: one modulus, one partition per remainder below it, a
    /// name that needs no quoting, and every probe where [`remainder`] puts it.
    pub(crate) fn from_catalog(
        rows: &[statement::stream_leaves::Row],
        probes: &[Uuid],
    ) -> Option<Self> {
        let modulus = u16::try_from(rows.first()?.modulus?).ok()?;
        if modulus == 0 || rows.len() != usize::from(modulus) {
            return None;
        }

        let mut names: Vec<Option<template::Ident>> = vec![None; rows.len()];
        let mut found = vec![0_usize; probes.len()];
        for row in rows {
            if row.modulus != Some(i32::from(modulus)) {
                return None;
            }
            let remainder = u16::try_from(row.remainder?).ok()?;
            let slot = names.get_mut(usize::from(remainder))?;
            if slot.is_some() {
                return None;
            }
            *slot = Some(template::Ident::new(row.leaf.as_str())?);

            for position in row.probes.as_deref()? {
                let index = usize::try_from(*position).ok()?.checked_sub(1)?;
                let probe = probes.get(index)?;
                if self::remainder(*probe, modulus) != remainder {
                    return None;
                }
                found[index] += 1;
            }
        }
        if found.iter().any(|count| *count != 1) {
            return None;
        }

        Some(Self {
            modulus,
            names: names.into_iter().collect::<Option<_>>()?,
        })
    }

    /// The partitions, by remainder.
    pub(crate) fn names(&self) -> &[template::Ident] {
        &self.names
    }

    /// The remainder of the partition holding `stream_id`: an index into [`Leaves::names`].
    pub(crate) fn index(&self, stream_id: Uuid) -> usize {
        usize::from(remainder(stream_id, self.modulus))
    }
}

/// The stream ids a store checks its routing with when it opens: fixed, spread over the hash space.
pub(crate) fn probes() -> Vec<Uuid> {
    // splitmix64, so the probes are fixed and their hashes cover the remainders of any modulus.
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };

    (0..PROBES)
        .map(|_| Uuid::from_u64_pair(next(), next()))
        .collect()
}

/// The partition a stream's row is in, for a store whose `streams` partitions all have `modulus`.
pub(crate) fn remainder(stream_id: Uuid, modulus: u16) -> u16 {
    let index = partition_hash(stream_id) % u64::from(modulus.max(1));

    u16::try_from(index).unwrap_or(0)
}

/// How many [`probes`] a store checks. The chance that a hash which differs from the server's still places
/// all of them is at most 2^-32 (modulus 2); a modulus of 1 routes everything to one partition anyway.
const PROBES: usize = 32;

/// `HASH_PARTITION_SEED` in PostgreSQL's `partition.h`.
const HASH_PARTITION_SEED: u64 = 0x7A5B_2236_7996_DCFD;

/// The constant of PostgreSQL's `hash_combine64`.
const HASH_COMBINE: u64 = 0x49a0_f4dd_15e5_a8e3;

/// `hash_combine64(0, uuid_hash_extended(id, HASH_PARTITION_SEED))`: one key column, starting from 0, so the
/// shifts of the accumulator vanish.
fn partition_hash(stream_id: Uuid) -> u64 {
    uuid_hash_extended(stream_id.as_bytes(), HASH_PARTITION_SEED).wrapping_add(HASH_COMBINE)
}

/// PostgreSQL's `hash_bytes_extended` for a 16-byte key, on a little-endian machine.
fn uuid_hash_extended(bytes: &[u8; 16], seed: u64) -> u64 {
    let word = |i: usize| u32::from_le_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    let init = 0x9e37_79b9_u32.wrapping_add(16).wrapping_add(3_923_095);
    let (mut a, mut b, mut c) = (init, init, init);

    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        mix(&mut a, &mut b, &mut c);
    }

    a = a.wrapping_add(word(0));
    b = b.wrapping_add(word(4));
    c = c.wrapping_add(word(8));
    mix(&mut a, &mut b, &mut c);

    a = a.wrapping_add(word(12));
    finish(&mut a, &mut b, &mut c);

    (u64::from(b) << 32) | u64::from(c)
}

fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(4);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(6);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(8);
    *b = b.wrapping_add(*a);
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(16);
    *c = c.wrapping_add(*b);
    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(19);
    *a = a.wrapping_add(*c);
    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(4);
    *b = b.wrapping_add(*a);
}

fn finish(a: &mut u32, b: &mut u32, c: &mut u32) {
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(14));
    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(11));
    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(25));
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(16));
    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(4));
    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(14));
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(24));
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use sqlx::{Connection, PgConnection};

    use super::*;

    /// Remainders PostgreSQL 18 gave for these ids in a 64-partition store (`satisfies_hash_partition`).
    #[test]
    fn known_remainders() {
        let cases = [
            ("00000000-0000-0000-0000-000000000000", 37),
            ("01a21800-4515-7311-919f-9fa361d91edb", 8),
            ("ffffffff-ffff-ffff-ffff-ffffffffffff", 1),
        ];
        for (id, expected) in cases {
            assert_eq!(
                remainder(Uuid::parse_str(id).unwrap(), 64),
                expected,
                "{id}"
            );
        }
    }

    /// Every remainder is below its modulus, and a modulus of 1 (or 0, which no store has) is one partition.
    #[test]
    fn remainders_are_below_the_modulus() {
        for probe in probes() {
            assert_eq!(remainder(probe, 0), 0);
            assert_eq!(remainder(probe, 1), 0);
            for modulus in [2, 3, 64, 256, u16::MAX] {
                assert!(remainder(probe, modulus) < modulus);
            }
        }
    }

    /// The probes land in many partitions, so a hash that differs from the server's misplaces some.
    #[test]
    fn probes_spread_over_the_partitions() {
        let partitions: std::collections::BTreeSet<u16> = probes()
            .into_iter()
            .map(|probe| remainder(probe, 64))
            .collect();

        assert!(partitions.len() >= 16, "{partitions:?}");
    }

    /// The rows `append/stream_leaves.sql` would return for a plain hash partitioning, from this module's hash.
    fn catalog(modulus: u16) -> Vec<statement::stream_leaves::Row> {
        let probes = probes();
        (0..modulus)
            .map(|remainder| statement::stream_leaves::Row {
                leaf: format!("streams_p{remainder:03}"),
                modulus: Some(i32::from(modulus)),
                remainder: Some(i32::from(remainder)),
                probes: Some(
                    (1..)
                        .zip(&probes)
                        .filter(|(_, probe)| self::remainder(**probe, modulus) == remainder)
                        .map(|(position, _)| position)
                        .collect(),
                ),
            })
            .collect()
    }

    #[test]
    fn a_plain_hash_partitioning_routes() {
        for modulus in [1, 2, 64, 256] {
            let leaves = Leaves::from_catalog(&catalog(modulus), &probes()).unwrap();

            assert_eq!(leaves.names().len(), usize::from(modulus));
            assert_eq!(leaves.names()[0].as_str(), "streams_p000");
            for probe in probes() {
                assert_eq!(leaves.index(probe), usize::from(remainder(probe, modulus)));
            }
        }
    }

    /// Anything this module cannot route exactly falls back to the parent table.
    #[test]
    fn other_partitionings_do_not_route() {
        let probes = probes();
        // The first partition holding a probe.
        let busy = |rows: &[statement::stream_leaves::Row]| {
            rows.iter()
                .position(|row| row.probes.as_ref().is_some_and(|held| !held.is_empty()))
                .unwrap()
        };
        let changed = |change: &dyn Fn(&mut Vec<statement::stream_leaves::Row>)| {
            let mut rows = catalog(64);
            change(&mut rows);
            Leaves::from_catalog(&rows, &probes)
        };

        assert!(Leaves::from_catalog(&[], &probes).is_none());
        // A partition missing (detached, or not a plain table).
        assert!(changed(&|rows| drop(rows.pop())).is_none());
        // Mixed moduli.
        assert!(changed(&|rows| rows[5].modulus = Some(32)).is_none());
        // A bound that is not a hash bound.
        assert!(changed(&|rows| rows[5].remainder = None).is_none());
        // Two partitions for one remainder.
        assert!(changed(&|rows| rows[5].remainder = Some(6)).is_none());
        // A name that needs quoting.
        assert!(changed(&|rows| rows[5].leaf = "Streams_5".to_owned()).is_none());
        // The server puts a probe elsewhere (another hash, such as a big-endian server's).
        assert!(
            changed(&|rows| {
                let from = busy(rows);
                let moved = rows[from].probes.as_mut().unwrap().pop().unwrap();
                rows[(from + 1) % 64].probes.as_mut().unwrap().push(moved);
            })
            .is_none()
        );
        // A probe the server put nowhere, or twice.
        assert!(
            changed(&|rows| {
                let at = busy(rows);
                rows[at].probes.as_mut().unwrap().pop();
            })
            .is_none()
        );
        assert!(
            changed(&|rows| {
                let at = busy(rows);
                let copy = rows[at].probes.as_ref().unwrap()[0];
                rows[at].probes.as_mut().unwrap().push(copy);
            })
            .is_none()
        );
    }

    /// For random ids, the remainder matches PostgreSQL's routing (`satisfies_hash_partition` against a table
    /// hash-partitioned on a uuid) for every modulus from 1 to 256.
    #[test]
    fn remainders_match_postgres() -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let db = runtime.block_on(async {
            let db = pges_testing::TestDb::new().await?;
            let mut conn = db.connect().await?;
            sqlx::query("CREATE TABLE h (id uuid) PARTITION BY HASH (id)")
                .execute(&mut conn)
                .await?;
            conn.close().await?;
            anyhow::Ok(db)
        })?;

        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64));
        let result = runner.run(&prop::collection::vec(any::<u128>(), 1..48), |ids| {
            let ids: Vec<Uuid> = ids.into_iter().map(Uuid::from_u128).collect();
            let mismatched = runtime
                .block_on(mismatches(db.url(), &ids))
                .map_err(|error| TestCaseError::fail(format!("{error:#}")))?;
            prop_assert!(mismatched.is_empty(), "{mismatched:?}");

            Ok(())
        });

        runtime.block_on(db.cleanup())?;
        result.map_err(|error| anyhow::anyhow!("{error}"))
    }

    /// Rows inserted into real partitionings land in the partition [`remainder`] names (`tableoid`).
    #[tokio::test]
    async fn inserted_rows_land_in_the_computed_partition() -> anyhow::Result<()> {
        let db = pges_testing::TestDb::new().await?;
        let mut conn = db.connect().await?;
        let ids: Vec<Uuid> = (0..2000).map(|_| Uuid::now_v7()).chain(probes()).collect();

        for modulus in [1_u16, 2, 3, 7, 64, 100, 256] {
            sqlx::query("DROP TABLE IF EXISTS h")
                .execute(&mut conn)
                .await?;
            sqlx::query("CREATE TABLE h (id uuid PRIMARY KEY) PARTITION BY HASH (id)")
                .execute(&mut conn)
                .await?;
            for remainder in 0..modulus {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE h_{remainder} PARTITION OF h \
                     FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})"
                )))
                .execute(&mut conn)
                .await?;
            }
            sqlx::query("INSERT INTO h SELECT unnest($1::uuid[])")
                .bind(&ids)
                .execute(&mut conn)
                .await?;

            let rows: Vec<(Uuid, String)> = sqlx::query_as(
                "SELECT h.id, c.relname::text FROM h JOIN pg_class c ON c.oid = h.tableoid",
            )
            .fetch_all(&mut conn)
            .await?;
            assert_eq!(rows.len(), ids.len());
            for (id, partition) in rows {
                assert_eq!(
                    partition,
                    format!("h_{}", remainder(id, modulus)),
                    "{id}, modulus {modulus}"
                );
            }
        }

        conn.close().await?;
        db.cleanup().await
    }

    /// The ids, and moduli from 1 to 256, for which `satisfies_hash_partition` disagrees with [`remainder`].
    async fn mismatches(url: &str, ids: &[Uuid]) -> anyhow::Result<Vec<(Uuid, i32)>> {
        let mut conn = PgConnection::connect(url).await?;
        let mut remainders = Vec::new();
        for id in ids {
            for modulus in 1..=256 {
                remainders.push(i32::from(remainder(*id, modulus)));
            }
        }

        let rows = sqlx::query_as(
            "SELECT u.id, m.modulus FROM unnest($1::uuid[]) WITH ORDINALITY AS u(id, ord)
             CROSS JOIN generate_series(1, 256) AS m(modulus)
             WHERE NOT satisfies_hash_partition('h'::regclass, m.modulus,
                                                ($2::int4[])[(u.ord - 1) * 256 + m.modulus], u.id)",
        )
        .bind(ids)
        .bind(&remainders)
        .fetch_all(&mut conn)
        .await?;
        conn.close().await?;

        Ok(rows)
    }
}
