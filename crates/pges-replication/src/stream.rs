//! The replication stream: XLogData and keepalives in, standby status updates out.

use std::{fmt, time::SystemTime};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::message::{backend, frontend};

use pges_core::lsn;

use crate::{connection, error, pgoutput, time, wire};

/// A running `START_REPLICATION`.
///
/// Read with [`ReplicationStream::next`] and report progress with [`ReplicationStream::confirm`]. The slot
/// keeps every transaction whose end LSN is above the confirmed LSN, and sends it again after a reconnect.
///
/// The server ends the connection if it hears nothing for `wal_sender_timeout` (60 s by default). Calling
/// `next` regularly is enough: it answers keepalives that ask for a reply. A consumer that stops reading for
/// longer should call [`ReplicationStream::send_status`] in between.
pub struct ReplicationStream {
    connection: connection::ReplicationConnection,
    received: lsn::Lsn,
    confirmed: lsn::Lsn,
    pending: Option<ReplicationMessage>,
}

impl ReplicationStream {
    /// The next message, or `None` once the server has ended streaming (after which only
    /// [`ReplicationStream::stop`] is useful).
    ///
    /// Keepalives that ask for a reply are answered with a status update (the last confirmed LSN) before they
    /// are returned.
    ///
    /// Cancel safe: if the future is dropped, no message is lost; the next call returns it, and a queued reply
    /// is sent by the next call on this stream.
    pub async fn next(&mut self) -> Result<Option<ReplicationMessage>, error::StreamError> {
        self.connection.wire.flush().await?;
        if let Some(message) = self.pending.take() {
            return Ok(Some(message));
        }

        loop {
            let body = match self.connection.wire.read().await? {
                wire::Backend::Message(backend::Message::CopyData(body)) => body.into_bytes(),
                wire::Backend::Message(backend::Message::CopyDone) => return Ok(None),
                wire::Backend::Message(backend::Message::ErrorResponse(body)) => {
                    return Err(error::server_error(&body)?.into());
                }
                wire::Backend::Message(
                    backend::Message::NoticeResponse(_) | backend::Message::ParameterStatus(_),
                ) => continue,
                _ => return Err(protocol("unexpected message while streaming")),
            };

            let message = parse(body)?;
            match &message {
                ReplicationMessage::XLogData(data) => {
                    self.received = self.received.max(data.wal_end);
                    return Ok(Some(message));
                }
                ReplicationMessage::Keepalive(keepalive) => {
                    if keepalive.reply == Reply::Requested {
                        self.queue_status(Reply::NotRequested);
                        // Stash first, so a cancelled flush cannot lose the keepalive.
                        self.pending = Some(message);
                        self.connection.wire.flush().await?;
                        return Ok(self.pending.take());
                    }

                    return Ok(Some(message));
                }
            }
        }
    }

    /// Confirms that everything up to `lsn` is durable on the consumer's side, and sends a status update.
    ///
    /// Pass a [`pgoutput::Commit::end_lsn`]: the slot then never sends that transaction, or any earlier one,
    /// again. The confirmed LSN never moves backwards; a lower `lsn` than already confirmed is ignored.
    ///
    /// Cancel safe: the update is queued before the first await; if the future is dropped, the next call on
    /// this stream sends it.
    pub async fn confirm(&mut self, lsn: lsn::Lsn) -> Result<(), error::StreamError> {
        self.confirmed = self.confirmed.max(lsn);
        self.queue_status(Reply::NotRequested);
        self.connection.wire.flush().await?;
        Ok(())
    }

    /// Sends a status update with the current positions without confirming anything new, e.g. as a heartbeat
    /// while the consumer is busy. With [`Reply::Requested`] the server answers with a keepalive.
    ///
    /// Cancel safe in the same way as [`ReplicationStream::confirm`].
    pub async fn send_status(&mut self, reply: Reply) -> Result<(), error::StreamError> {
        self.queue_status(reply);
        self.connection.wire.flush().await?;
        Ok(())
    }

    /// The highest LSN confirmed so far (initially the start LSN given to `START_REPLICATION`).
    pub fn confirmed(&self) -> lsn::Lsn {
        self.confirmed
    }

    /// The highest XLogData LSN received so far.
    pub fn received(&self) -> lsn::Lsn {
        self.received
    }

    /// Ends streaming (`CopyDone`), discards anything still in flight, and returns the connection for more
    /// commands. Unconfirmed transactions are sent again by the next `START_REPLICATION`.
    ///
    /// Not cancel safe: dropping the future drops the connection, which also ends streaming.
    pub async fn stop(mut self) -> Result<connection::ReplicationConnection, error::StreamError> {
        self.pending = None;
        frontend::copy_done(&mut self.connection.wire.write_buf);
        self.connection.wire.flush().await?;

        let mut server_error = None;
        loop {
            match self.connection.wire.read().await? {
                wire::Backend::Message(backend::Message::ReadyForQuery(_)) => break,
                wire::Backend::Message(backend::Message::ErrorResponse(body)) => {
                    let error = error::server_error(&body)?;
                    // FATAL and PANIC close the socket without ReadyForQuery.
                    if error.ends_session() {
                        return Err(error.into());
                    }
                    server_error = Some(error);
                }
                wire::Backend::Message(
                    backend::Message::CopyData(_)
                    | backend::Message::CopyDone
                    | backend::Message::CommandComplete(_)
                    | backend::Message::RowDescription(_)
                    | backend::Message::DataRow(_)
                    | backend::Message::NoticeResponse(_)
                    | backend::Message::ParameterStatus(_),
                ) => {}
                _ => return Err(protocol("unexpected message while stopping")),
            }
        }

        if let Some(error) = server_error {
            return Err(error.into());
        }
        self.connection.set_idle();
        Ok(self.connection)
    }

    pub(crate) fn new(connection: connection::ReplicationConnection, start: lsn::Lsn) -> Self {
        Self {
            connection,
            received: start,
            confirmed: start,
            pending: None,
        }
    }

    fn queue_status(&mut self, reply: Reply) {
        let mut status = BytesMut::with_capacity(STATUS_LEN);
        status.put_u8(b'r');
        status.put_u64(self.received.max(self.confirmed).get());
        status.put_u64(self.confirmed.get());
        status.put_u64(self.confirmed.get());
        status.put_i64(time::to_postgres(SystemTime::now()));
        status.put_u8(match reply {
            Reply::Requested => 1,
            Reply::NotRequested => 0,
        });
        // A CopyData of a fixed 34-byte body cannot exceed the message size limit.
        if let Ok(data) = frontend::CopyData::new(status.freeze()) {
            data.write(&mut self.connection.wire.write_buf);
        }
    }
}

impl fmt::Debug for ReplicationStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationStream")
            .field("received", &self.received)
            .field("confirmed", &self.confirmed)
            .finish_non_exhaustive()
    }
}

/// A message from the server while streaming.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ReplicationMessage {
    /// A decoded change.
    XLogData(XLogData),
    /// A primary keepalive.
    Keepalive(Keepalive),
}

/// One `pgoutput` message with its WAL position.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct XLogData {
    /// The LSN of the WAL record the message was decoded from.
    pub wal_start: lsn::Lsn,
    /// For logical replication the server sends the same LSN as `wal_start`.
    pub wal_end: lsn::Lsn,
    /// When the server sent the message.
    pub server_time: SystemTime,
    /// The decoded message.
    pub message: pgoutput::Message,
}

/// A primary keepalive.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Keepalive {
    /// The server's current WAL send position. Once every message received before this keepalive is durable
    /// and no transaction is open (no Begin without its Commit), confirming this LSN is safe; it lets the slot
    /// move past WAL that contained nothing for the publications.
    pub wal_end: lsn::Lsn,
    /// When the server sent it.
    pub server_time: SystemTime,
    /// Whether the server asked for an immediate status update ([`ReplicationStream::next`] has already sent
    /// it).
    pub reply: Reply,
}

/// Whether a status update or keepalive asks the other side to answer at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// Answer immediately.
    Requested,
    /// No answer needed.
    NotRequested,
}

const STATUS_LEN: usize = 34;
const XLOG_DATA_HEADER_LEN: usize = 24;
const KEEPALIVE_LEN: usize = 17;

fn parse(mut body: Bytes) -> Result<ReplicationMessage, error::StreamError> {
    if body.is_empty() {
        return Err(protocol("empty CopyData"));
    }

    match body.get_u8() {
        b'w' => {
            if body.len() < XLOG_DATA_HEADER_LEN {
                return Err(protocol("truncated XLogData header"));
            }
            let wal_start = lsn::Lsn::new(body.get_u64());
            let wal_end = lsn::Lsn::new(body.get_u64());
            let server_time = time::from_postgres(body.get_i64());
            Ok(ReplicationMessage::XLogData(XLogData {
                wal_start,
                wal_end,
                server_time,
                message: pgoutput::Message::decode(body)?,
            }))
        }
        b'k' => {
            if body.len() != KEEPALIVE_LEN {
                return Err(protocol("keepalive of the wrong length"));
            }
            Ok(ReplicationMessage::Keepalive(Keepalive {
                wal_end: lsn::Lsn::new(body.get_u64()),
                server_time: time::from_postgres(body.get_i64()),
                reply: if body.get_u8() == 1 {
                    Reply::Requested
                } else {
                    Reply::NotRequested
                },
            }))
        }
        _ => Err(protocol("unknown CopyData message while streaming")),
    }
}

fn protocol(message: &str) -> error::StreamError {
    error::StreamError::Protocol(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_keepalive() {
        let mut body = BytesMut::new();
        body.put_u8(b'k');
        body.put_u64(0x42);
        body.put_i64(0);
        body.put_u8(1);
        let Ok(ReplicationMessage::Keepalive(keepalive)) = parse(body.freeze()) else {
            panic!("not a keepalive");
        };
        assert_eq!(keepalive.wal_end, lsn::Lsn::new(0x42));
        assert_eq!(keepalive.reply, Reply::Requested);
    }

    #[test]
    fn parses_xlog_data() {
        let mut body = BytesMut::new();
        body.put_u8(b'w');
        body.put_u64(7);
        body.put_u64(7);
        body.put_i64(0);
        body.put_slice(b"O\0\0\0\0\0\0\0\x09n\0");
        let Ok(ReplicationMessage::XLogData(data)) = parse(body.freeze()) else {
            panic!("not XLogData");
        };
        assert_eq!(data.wal_start, lsn::Lsn::new(7));
        assert!(matches!(data.message, pgoutput::Message::Origin(_)));
    }

    #[test]
    fn rejects_truncated_frames() {
        assert!(parse(Bytes::from_static(b"w\0\0")).is_err());
        assert!(parse(Bytes::from_static(b"k\0")).is_err());
        assert!(parse(Bytes::from_static(b"x")).is_err());
        assert!(parse(Bytes::new()).is_err());
    }

    proptest::proptest! {
        /// CopyData from the server never panics the parser: arbitrary bytes after each tag, at lengths
        /// around the XLogData header and the keepalive.
        #[test]
        fn parse_never_panics(
            tag in proptest::sample::select(b"wkx".as_slice()),
            body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        ) {
            let mut bytes = vec![tag];
            bytes.extend_from_slice(&body);

            let _ = parse(Bytes::from(bytes));
        }
    }
}
