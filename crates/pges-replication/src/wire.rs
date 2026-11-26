//! Socket, framing and buffering shared by the connection and the stream.
//!
//! Reads go through `read_buf` and writes through `write_buf`, and every await point only moves bytes between
//! a buffer and the socket. A future that is dropped mid-way therefore loses nothing: unread messages stay in
//! `read_buf`, and unsent bytes stay in `write_buf` until the next [`Wire::flush`].

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use postgres_protocol::message::backend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

#[cfg(feature = "tls")]
use crate::tls;
use crate::{config, connection, error};

/// A backend message, including `CopyBothResponse`, which `postgres-protocol` does not parse.
pub(crate) enum Backend {
    Message(backend::Message),
    CopyBothResponse,
}

/// Whether one connection attempt over TCP uses TLS. Unix sockets never do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tls {
    Off,
    /// TLS if the server offers it, otherwise plain on the same socket.
    #[cfg(feature = "tls")]
    IfOffered,
    #[cfg(feature = "tls")]
    Required,
}

pub(crate) struct Wire {
    io: Transport,
    read_buf: BytesMut,
    pub(crate) write_buf: BytesMut,
}

impl Wire {
    pub(crate) async fn connect(
        config: &config::Config,
        tls: Tls,
    ) -> Result<Self, error::ConnectError> {
        let io = match config.get_host() {
            config::Host::Tcp(host) => {
                let stream =
                    tokio::net::TcpStream::connect((host.as_str(), config.get_port())).await?;
                stream.set_nodelay(true)?;
                match tls {
                    Tls::Off => Transport::Tcp(stream),
                    #[cfg(feature = "tls")]
                    Tls::IfOffered | Tls::Required => {
                        match tls::connect(stream, host, config).await? {
                            tls::Negotiated::Encrypted(stream) => Transport::Tls(stream),
                            tls::Negotiated::Declined(stream) if tls == Tls::IfOffered => {
                                Transport::Tcp(stream)
                            }
                            tls::Negotiated::Declined(_) => {
                                return Err(error::TlsError::NotOffered.into());
                            }
                        }
                    }
                }
            }
            #[cfg(unix)]
            config::Host::Unix(directory) => {
                let path = directory.join(format!(".s.PGSQL.{}", config.get_port()));
                Transport::Unix(tokio::net::UnixStream::connect(path).await?)
            }
            #[cfg(not(unix))]
            config::Host::Unix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unix sockets are not available on this platform",
                ));
            }
        };
        Ok(Self {
            io,
            read_buf: BytesMut::with_capacity(READ_CAPACITY),
            write_buf: BytesMut::new(),
        })
    }

    pub(crate) fn encryption(&self) -> connection::Encryption {
        match self.io {
            Transport::Tcp(_) => connection::Encryption::Plain,
            #[cfg(unix)]
            Transport::Unix(_) => connection::Encryption::Plain,
            #[cfg(feature = "tls")]
            Transport::Tls(_) => connection::Encryption::Tls,
        }
    }

    /// The `tls-server-end-point` channel binding data, when the connection is TLS and the server
    /// certificate's signature algorithm defines it.
    pub(crate) fn tls_server_end_point(&self) -> Option<Vec<u8>> {
        match &self.io {
            #[cfg(feature = "tls")]
            Transport::Tls(stream) => {
                let (_, session) = stream.get_ref();
                let certificate = session.peer_certificates()?.first()?;
                tls::server_end_point(certificate)
            }
            _ => None,
        }
    }

    /// Sends everything in `write_buf`. Cancel safe: unsent bytes stay buffered.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        while self.write_buf.has_remaining() {
            let written = self.io.write_buf(&mut self.write_buf).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
        }
        self.io.flush().await
    }

    /// The next backend message. Cancel safe: partial messages stay buffered.
    pub(crate) async fn read(&mut self) -> io::Result<Backend> {
        loop {
            if let Some(message) = self.parse()? {
                return Ok(message);
            }

            if self.io.read_buf(&mut self.read_buf).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the server closed the connection",
                ));
            }
        }
    }

    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        self.flush().await?;
        self.io.shutdown().await
    }

    fn parse(&mut self) -> io::Result<Option<Backend>> {
        let Some(header) = backend::Header::parse(&self.read_buf)? else {
            return Ok(None);
        };

        if header.tag() != COPY_BOTH_RESPONSE_TAG {
            return Ok(backend::Message::parse(&mut self.read_buf)?.map(Backend::Message));
        }

        // The length counts itself but not the tag.
        let total = 1 + usize::try_from(header.len()).unwrap_or(0);
        if self.read_buf.len() < total {
            self.read_buf.reserve(total - self.read_buf.len());
            return Ok(None);
        }
        self.read_buf.advance(total);
        Ok(Some(Backend::CopyBothResponse))
    }
}

const COPY_BOTH_RESPONSE_TAG: u8 = b'W';
const READ_CAPACITY: usize = 64 * 1024;

enum Transport {
    Tcp(tokio::net::TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
