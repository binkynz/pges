//! Decoding of `pgoutput` messages, the payload of XLogData.
//!
//! The decoder covers protocol versions 1 to 4 for transactions that are not streamed (`streaming 'off'`) and
//! not prepared (`two_phase` off). Streaming and two-phase messages are rejected with
//! [`DecodeError::Unsupported`], because the sequencer never asks for them.
//!
//! Relations are announced with a [`Relation`] message before the first change that uses them, and again
//! after their definition changes. Changes name their table only by [`RelationId`], so a consumer keeps the
//! latest [`Relation`] per id, for example in [`Relations`]. With `publish_via_partition_root`, PostgreSQL 18
//! sends [`Relation`] messages for leaf partitions as well as the root, while the changes themselves carry
//! the root's id: route by id, never by assuming which names appear.

use std::{collections::HashMap, fmt, str, time::SystemTime};

use bytes::{Buf, Bytes};

use pges_core::lsn;

use crate::time;

/// One `pgoutput` message.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    /// A transaction starts.
    Begin(Begin),
    /// The transaction ends; everything between [`Message::Begin`] and this belongs to it.
    Commit(Commit),
    /// The transaction was replayed from another node.
    Origin(Origin),
    /// A table's definition, sent before its first change in the session and after it changes.
    Relation(Relation),
    /// A data type's name, sent before a [`Relation`] that uses a non-built-in type.
    Type(Type),
    /// A row was inserted.
    Insert(Insert),
    /// A row was updated.
    Update(Update),
    /// A row was deleted.
    Delete(Delete),
    /// Tables were truncated.
    Truncate(Truncate),
    /// A message emitted with `pg_logical_emit_message`; only sent when replication starts with
    /// [`Messages::Include`](crate::connection::Messages::Include).
    Logical(LogicalMessage),
}

impl Message {
    /// Decodes one XLogData payload.
    pub fn decode(bytes: Bytes) -> Result<Self, DecodeError> {
        let mut reader = Reader(bytes);
        let tag = reader.u8()?;
        let message = match tag {
            b'B' => Self::Begin(Begin {
                final_lsn: reader.lsn()?,
                commit_time: reader.time()?,
                xid: TransactionId(reader.u32()?),
            }),
            b'C' => {
                reader.u8()?;
                Self::Commit(Commit {
                    commit_lsn: reader.lsn()?,
                    end_lsn: reader.lsn()?,
                    commit_time: reader.time()?,
                })
            }
            b'O' => Self::Origin(Origin {
                commit_lsn: reader.lsn()?,
                name: reader.string()?,
            }),
            b'R' => Self::Relation(reader.relation()?),
            b'Y' => Self::Type(Type {
                id: TypeId(reader.u32()?),
                namespace: reader.string()?,
                name: reader.string()?,
            }),
            b'I' => {
                let relation = RelationId(reader.u32()?);
                reader.expect(b'N', "new tuple marker")?;
                Self::Insert(Insert {
                    relation,
                    new: reader.tuple()?,
                })
            }
            b'U' => {
                let relation = RelationId(reader.u32()?);
                let old = match reader.u8()? {
                    b'K' => {
                        let old = OldRow::Key(reader.tuple()?);
                        reader.expect(b'N', "new tuple marker")?;
                        Some(old)
                    }
                    b'O' => {
                        let old = OldRow::Full(reader.tuple()?);
                        reader.expect(b'N', "new tuple marker")?;
                        Some(old)
                    }
                    b'N' => None,
                    other => return Err(DecodeError::Invalid("update tuple marker", other)),
                };
                Self::Update(Update {
                    relation,
                    old,
                    new: reader.tuple()?,
                })
            }
            b'D' => {
                let relation = RelationId(reader.u32()?);
                let old = match reader.u8()? {
                    b'K' => OldRow::Key(reader.tuple()?),
                    b'O' => OldRow::Full(reader.tuple()?),
                    other => return Err(DecodeError::Invalid("delete tuple marker", other)),
                };
                Self::Delete(Delete { relation, old })
            }
            b'T' => {
                let count = reader.u32()?;
                let options = reader.u8()?;
                let relations = (0..count)
                    .map(|_| reader.u32().map(RelationId))
                    .collect::<Result<_, _>>()?;
                Self::Truncate(Truncate {
                    relations,
                    cascade: if options & 1 != 0 {
                        Cascade::Cascade
                    } else {
                        Cascade::Restrict
                    },
                    identity: if options & 2 != 0 {
                        Identity::Restart
                    } else {
                        Identity::Continue
                    },
                })
            }
            b'M' => {
                let flags = reader.u8()?;
                let lsn = reader.lsn()?;
                let prefix = reader.string()?;
                let len = reader.u32()? as usize;
                Self::Logical(LogicalMessage {
                    transactional: if flags & 1 != 0 {
                        Transactional::Transactional
                    } else {
                        Transactional::NonTransactional
                    },
                    lsn,
                    prefix,
                    content: reader.bytes(len)?,
                })
            }
            b'S' | b'E' | b'c' | b'A' | b'b' | b'P' | b'K' | b'r' | b'p' => {
                return Err(DecodeError::Unsupported(tag));
            }
            other => return Err(DecodeError::UnknownMessage(other)),
        };

        if reader.0.has_remaining() {
            return Err(DecodeError::TrailingBytes(tag));
        }
        Ok(message)
    }
}

/// The start of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Begin {
    /// The LSN of the transaction's commit record (equal to [`Commit::commit_lsn`]).
    pub final_lsn: lsn::Lsn,
    /// When the transaction committed.
    pub commit_time: SystemTime,
    /// The transaction id.
    pub xid: TransactionId,
}

/// The end of a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Commit {
    /// The LSN of the commit record.
    pub commit_lsn: lsn::Lsn,
    /// The LSN just after the commit record. Confirming this LSN to the slot means the transaction is never
    /// sent again.
    pub end_lsn: lsn::Lsn,
    /// When the transaction committed.
    pub commit_time: SystemTime,
}

/// The origin of a replayed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Origin {
    /// The commit LSN on the origin server.
    pub commit_lsn: lsn::Lsn,
    /// The origin's name.
    pub name: String,
}

/// A table's definition.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Relation {
    /// The table's OID, which later changes refer to.
    pub id: RelationId,
    /// The schema.
    pub namespace: String,
    /// The table name.
    pub name: String,
    /// Which old values updates and deletes carry.
    pub replica_identity: ReplicaIdentity,
    /// The published columns, in the order tuples carry them.
    pub columns: Vec<Column>,
}

/// A table's OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub u32);

impl fmt::Display for RelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A transaction id (`xid`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionId(pub u32);

/// A data type's OID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeId(pub u32);

/// `REPLICA IDENTITY` of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicaIdentity {
    /// The primary key.
    Default,
    /// No old values.
    Nothing,
    /// All columns.
    Full,
    /// The columns of a chosen unique index.
    Index,
}

/// One published column.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Column {
    /// Whether the column is part of the replica identity.
    pub key: KeyColumn,
    /// The column name.
    pub name: String,
    /// The column's data type.
    pub type_id: TypeId,
    /// The type modifier (`atttypmod`), -1 when there is none.
    pub type_modifier: i32,
}

/// Whether a column belongs to the replica identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyColumn {
    /// Part of the replica identity.
    Key,
    /// Not part of it.
    NotKey,
}

/// A data type's name.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Type {
    /// The type's OID.
    pub id: TypeId,
    /// The schema.
    pub namespace: String,
    /// The type name.
    pub name: String,
}

/// An inserted row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Insert {
    /// The table, as announced by a [`Relation`] message.
    pub relation: RelationId,
    /// The new row.
    pub new: Tuple,
}

/// An updated row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Update {
    /// The table.
    pub relation: RelationId,
    /// The old key or row, when the replica identity provides one and it changed.
    pub old: Option<OldRow>,
    /// The new row.
    pub new: Tuple,
}

/// A deleted row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delete {
    /// The table.
    pub relation: RelationId,
    /// The deleted key or row.
    pub old: OldRow,
}

/// The old values of an updated or deleted row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OldRow {
    /// Only the replica identity columns are set; the others are null.
    Key(Tuple),
    /// The whole row (`REPLICA IDENTITY FULL`).
    Full(Tuple),
}

/// Truncated tables.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Truncate {
    /// The tables.
    pub relations: Vec<RelationId>,
    /// Whether `CASCADE` was given.
    pub cascade: Cascade,
    /// Whether `RESTART IDENTITY` was given.
    pub identity: Identity,
}

/// `TRUNCATE … CASCADE` or `RESTRICT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Cascade {
    /// `CASCADE`.
    Cascade,
    /// `RESTRICT`, the default.
    Restrict,
}

/// `TRUNCATE … RESTART IDENTITY` or `CONTINUE IDENTITY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Identity {
    /// `RESTART IDENTITY`.
    Restart,
    /// `CONTINUE IDENTITY`, the default.
    Continue,
}

/// A message from `pg_logical_emit_message`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LogicalMessage {
    /// Whether the message belongs to the surrounding transaction.
    pub transactional: Transactional,
    /// The LSN of the message record.
    pub lsn: lsn::Lsn,
    /// The prefix given when emitting it.
    pub prefix: String,
    /// The content.
    pub content: Bytes,
}

/// Whether a logical message is part of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transactional {
    /// Sent between Begin and Commit, only if the transaction commits.
    Transactional,
    /// Sent immediately, outside any transaction.
    NonTransactional,
}

/// The column values of a row, in the order of the [`Relation`]'s columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple(pub Vec<Value>);

impl Tuple {
    /// The value of column `index`.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }

    /// The text value of column `index`, or `None` when it is out of range, null, unchanged or binary, or not
    /// UTF-8.
    pub fn text(&self, index: usize) -> Option<&str> {
        match self.get(index)? {
            Value::Text(bytes) => str::from_utf8(bytes).ok(),
            Value::Null | Value::UnchangedToast | Value::Binary(_) => None,
        }
    }
}

/// One column value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Value {
    /// SQL null.
    Null,
    /// A TOASTed value that the change did not touch; its content is not sent.
    UnchangedToast,
    /// The value in the type's text output format (the server encoding; the connection asks for UTF-8).
    Text(Bytes),
    /// The value in the type's binary format; only sent when replication asks for `binary`.
    Binary(Bytes),
}

/// The latest [`Relation`] per [`RelationId`], kept up to date from the stream.
#[derive(Debug, Clone, Default)]
pub struct Relations(HashMap<RelationId, Relation>);

impl Relations {
    /// An empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `relation`, replacing any earlier definition with the same id.
    pub fn insert(&mut self, relation: Relation) {
        self.0.insert(relation.id, relation);
    }

    /// The latest definition announced for `id`.
    pub fn get(&self, id: RelationId) -> Option<&Relation> {
        self.0.get(&id)
    }
}

/// An XLogData payload is not a valid `pgoutput` message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The message ended early.
    #[error("pgoutput message is truncated")]
    Truncated,
    /// Bytes are left after the message.
    #[error("pgoutput message {:?} has trailing bytes", char::from(*.0))]
    TrailingBytes(u8),
    /// The message type is not one pgoutput sends.
    #[error("unknown pgoutput message type {:?}", char::from(*.0))]
    UnknownMessage(u8),
    /// A streaming or two-phase message, which is only sent when replication asks for them.
    #[error("pgoutput message type {:?} (streaming or two-phase) is not supported", char::from(*.0))]
    Unsupported(u8),
    /// A field holds a byte the protocol does not allow there.
    #[error("invalid {}: {:?}", .0, char::from(*.1))]
    Invalid(&'static str, u8),
    /// A name is not valid UTF-8.
    #[error("pgoutput string is not valid UTF-8")]
    InvalidString,
}

struct Reader(Bytes);

impl Reader {
    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.0.remaining() < n {
            return Err(DecodeError::Truncated);
        }

        Ok(())
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        self.need(1)?;
        Ok(self.0.get_u8())
    }

    fn expect(&mut self, byte: u8, what: &'static str) -> Result<(), DecodeError> {
        let got = self.u8()?;
        if got != byte {
            return Err(DecodeError::Invalid(what, got));
        }

        Ok(())
    }

    fn i16(&mut self) -> Result<i16, DecodeError> {
        self.need(2)?;
        Ok(self.0.get_i16())
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        self.need(4)?;
        Ok(self.0.get_u32())
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        self.need(4)?;
        Ok(self.0.get_i32())
    }

    fn lsn(&mut self) -> Result<lsn::Lsn, DecodeError> {
        self.need(8)?;
        Ok(lsn::Lsn::new(self.0.get_u64()))
    }

    fn time(&mut self) -> Result<SystemTime, DecodeError> {
        self.need(8)?;
        Ok(time::from_postgres(self.0.get_i64()))
    }

    fn bytes(&mut self, len: usize) -> Result<Bytes, DecodeError> {
        self.need(len)?;
        Ok(self.0.split_to(len))
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        let end = self
            .0
            .iter()
            .position(|b| *b == 0)
            .ok_or(DecodeError::Truncated)?;
        let text = self.0.split_to(end);
        self.0.advance(1);
        String::from_utf8(text.to_vec()).map_err(|_| DecodeError::InvalidString)
    }

    fn relation(&mut self) -> Result<Relation, DecodeError> {
        let id = RelationId(self.u32()?);
        let namespace = self.string()?;
        let name = self.string()?;
        let replica_identity = match self.u8()? {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::Nothing,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            other => return Err(DecodeError::Invalid("replica identity", other)),
        };
        let count = self.i16()?;
        let mut columns = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let flags = self.u8()?;
            columns.push(Column {
                key: if flags & 1 != 0 {
                    KeyColumn::Key
                } else {
                    KeyColumn::NotKey
                },
                name: self.string()?,
                type_id: TypeId(self.u32()?),
                type_modifier: self.i32()?,
            });
        }
        Ok(Relation {
            id,
            namespace,
            name,
            replica_identity,
            columns,
        })
    }

    fn tuple(&mut self) -> Result<Tuple, DecodeError> {
        let count = self.i16()?;
        let mut values = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let value = match self.u8()? {
                b'n' => Value::Null,
                b'u' => Value::UnchangedToast,
                b't' => {
                    let len = self.u32()? as usize;
                    Value::Text(self.bytes(len)?)
                }
                b'b' => {
                    let len = self.u32()? as usize;
                    Value::Binary(self.bytes(len)?)
                }
                other => return Err(DecodeError::Invalid("column value kind", other)),
            };
            values.push(value);
        }
        Ok(Tuple(values))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use bytes::{BufMut, BytesMut};

    use super::*;

    fn cstr(buf: &mut BytesMut, s: &str) {
        buf.put_slice(s.as_bytes());
        buf.put_u8(0);
    }

    /// 2000-01-01 plus one second, in microseconds since the PostgreSQL epoch.
    const ONE_SECOND: i64 = 1_000_000;
    const ONE_SECOND_AFTER_PG_EPOCH: Duration = Duration::from_secs(946_684_801);

    #[test]
    fn decodes_begin_and_commit() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'B');
        buf.put_u64(0x10);
        buf.put_i64(ONE_SECOND);
        buf.put_u32(742);
        assert_eq!(
            Message::decode(buf.freeze()),
            Ok(Message::Begin(Begin {
                final_lsn: lsn::Lsn::new(0x10),
                commit_time: UNIX_EPOCH + ONE_SECOND_AFTER_PG_EPOCH,
                xid: TransactionId(742),
            }))
        );

        let mut buf = BytesMut::new();
        buf.put_u8(b'C');
        buf.put_u8(0);
        buf.put_u64(0x10);
        buf.put_u64(0x38);
        buf.put_i64(ONE_SECOND);
        assert_eq!(
            Message::decode(buf.freeze()),
            Ok(Message::Commit(Commit {
                commit_lsn: lsn::Lsn::new(0x10),
                end_lsn: lsn::Lsn::new(0x38),
                commit_time: UNIX_EPOCH + ONE_SECOND_AFTER_PG_EPOCH,
            }))
        );
    }

    #[test]
    fn decodes_relation_and_insert() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'R');
        buf.put_u32(16_400);
        cstr(&mut buf, "s1");
        cstr(&mut buf, "commits");
        buf.put_u8(b'd');
        buf.put_i16(2);
        buf.put_u8(1);
        cstr(&mut buf, "commit_id");
        buf.put_u32(2950);
        buf.put_i32(-1);
        buf.put_u8(0);
        cstr(&mut buf, "event_count");
        buf.put_u32(21);
        buf.put_i32(-1);
        let Ok(Message::Relation(relation)) = Message::decode(buf.freeze()) else {
            panic!("not a relation");
        };
        assert_eq!(relation.id, RelationId(16_400));
        assert_eq!(relation.name, "commits");
        assert_eq!(relation.columns[0].key, KeyColumn::Key);
        assert_eq!(relation.columns[1].name, "event_count");
        assert_eq!(relation.columns[1].type_id, TypeId(21));

        let mut buf = BytesMut::new();
        buf.put_u8(b'I');
        buf.put_u32(16_400);
        buf.put_u8(b'N');
        buf.put_i16(3);
        buf.put_u8(b't');
        buf.put_u32(1);
        buf.put_slice(b"7");
        buf.put_u8(b'n');
        buf.put_u8(b'u');
        let Ok(Message::Insert(insert)) = Message::decode(buf.freeze()) else {
            panic!("not an insert");
        };
        assert_eq!(insert.relation, RelationId(16_400));
        assert_eq!(insert.new.text(0), Some("7"));
        assert_eq!(insert.new.get(1), Some(&Value::Null));
        assert_eq!(insert.new.get(2), Some(&Value::UnchangedToast));
        assert_eq!(insert.new.text(3), None);
    }

    #[test]
    fn decodes_update_delete_truncate_message_origin_type() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'U');
        buf.put_u32(1);
        buf.put_u8(b'K');
        buf.put_i16(1);
        buf.put_u8(b'n');
        buf.put_u8(b'N');
        buf.put_i16(1);
        buf.put_u8(b'n');
        assert!(matches!(
            Message::decode(buf.freeze()),
            Ok(Message::Update(Update {
                old: Some(OldRow::Key(_)),
                ..
            }))
        ));

        let mut buf = BytesMut::new();
        buf.put_u8(b'D');
        buf.put_u32(1);
        buf.put_u8(b'O');
        buf.put_i16(0);
        assert!(matches!(
            Message::decode(buf.freeze()),
            Ok(Message::Delete(Delete {
                old: OldRow::Full(_),
                ..
            }))
        ));

        let mut buf = BytesMut::new();
        buf.put_u8(b'T');
        buf.put_u32(2);
        buf.put_u8(3);
        buf.put_u32(5);
        buf.put_u32(6);
        assert_eq!(
            Message::decode(buf.freeze()),
            Ok(Message::Truncate(Truncate {
                relations: vec![RelationId(5), RelationId(6)],
                cascade: Cascade::Cascade,
                identity: Identity::Restart,
            }))
        );

        let mut buf = BytesMut::new();
        buf.put_u8(b'M');
        buf.put_u8(1);
        buf.put_u64(9);
        cstr(&mut buf, "pges");
        buf.put_u32(3);
        buf.put_slice(b"abc");
        assert_eq!(
            Message::decode(buf.freeze()),
            Ok(Message::Logical(LogicalMessage {
                transactional: Transactional::Transactional,
                lsn: lsn::Lsn::new(9),
                prefix: "pges".into(),
                content: Bytes::from_static(b"abc"),
            }))
        );

        let mut buf = BytesMut::new();
        buf.put_u8(b'O');
        buf.put_u64(9);
        cstr(&mut buf, "node_a");
        assert!(matches!(
            Message::decode(buf.freeze()),
            Ok(Message::Origin(_))
        ));

        let mut buf = BytesMut::new();
        buf.put_u8(b'Y');
        buf.put_u32(77);
        cstr(&mut buf, "public");
        cstr(&mut buf, "mood");
        assert!(matches!(
            Message::decode(buf.freeze()),
            Ok(Message::Type(_))
        ));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(
            Message::decode(Bytes::from_static(b"B\0\0")),
            Err(DecodeError::Truncated)
        );
        assert_eq!(
            Message::decode(Bytes::from_static(b"S")),
            Err(DecodeError::Unsupported(b'S'))
        );
        assert_eq!(
            Message::decode(Bytes::from_static(b"z")),
            Err(DecodeError::UnknownMessage(b'z'))
        );
        assert_eq!(Message::decode(Bytes::new()), Err(DecodeError::Truncated));

        let mut buf = BytesMut::new();
        buf.put_u8(b'O');
        buf.put_u64(9);
        cstr(&mut buf, "x");
        buf.put_u8(0);
        assert_eq!(
            Message::decode(buf.freeze()),
            Err(DecodeError::TrailingBytes(b'O'))
        );
    }
}
