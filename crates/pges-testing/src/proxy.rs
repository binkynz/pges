//! Fault injection: a TCP proxy in front of the test server.
//!
//! Tests connect through [`Proxy::url`] instead of [`crate::TestDb::url`], then tell the proxy to
//! [`Proxy::drop_connections`] (every connection open now is closed, as if the network failed) or to
//! [`Proxy::hold`] the bytes going one way (the client waits, as if a response was slow or lost) until
//! [`Proxy::release`]. [`Proxy::refuse`] closes new connections as soon as they arrive, until
//! [`Proxy::admit`], and [`Proxy::accepted`] counts the connections that arrived. The proxy listens on
//! `127.0.0.1` and forwards to the server over TCP or its unix socket. [`Proxy::to`] puts it in front of any
//! other TCP server instead, such as a sequencer's gRPC watch.
//!
//! ```no_run
//! # async fn example() -> anyhow::Result<()> {
//! use pges_testing::proxy;
//!
//! let db = pges_testing::TestDb::new().await?;
//! let proxy = proxy::Proxy::start(pges_testing::server().await?).await?;
//! let pool = sqlx::postgres::PgPoolOptions::new()
//!     .test_before_acquire(false)
//!     .connect(&proxy.url(&db)?)
//!     .await?;
//!
//! proxy.hold(proxy::Direction::ToClient); // statements still reach the server; their responses wait
//! proxy.drop_connections();                // and are lost
//! proxy.release();
//! # Ok(())
//! # }
//! ```
//!
//! sqlx itself talks to the server around a statement, and while responses are held it waits for those
//! answers too:
//!
//! - a pool pings a connection before handing it out, unless built with `test_before_acquire(false)`, and
//!   always when it takes one back (wait for `pool.num_idle()` before holding);
//! - a connection prepares a statement the first time it runs it and waits for the answer before executing
//!   it, so run the statement once before holding the responses to it.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    sync::watch,
    task::JoinHandle,
};

use crate::{Server, TestDb};

/// A TCP proxy to the test server that can drop connections and hold traffic. Dropping it closes every
/// connection through it.
pub struct Proxy {
    addr: SocketAddr,
    upstream: Upstream,
    control: watch::Sender<Control>,
    accepted: Arc<AtomicU64>,
    accept: JoinHandle<()>,
}

impl Proxy {
    /// Starts a proxy to `server` on a free local port.
    pub async fn start(server: &Server) -> Result<Self> {
        Self::with_upstream(Upstream::of(server.admin_url())?).await
    }

    /// Starts a proxy to any TCP address, such as a gRPC server, on a free local port. [`Proxy::url`] makes
    /// no sense for it; connect to [`Proxy::addr`].
    pub async fn to(addr: SocketAddr) -> Result<Self> {
        Self::with_upstream(Upstream::Tcp(addr.to_string())).await
    }

    async fn with_upstream(upstream: Upstream) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .context("binding the proxy")?;
        let addr = listener.local_addr()?;
        let (control, _) = watch::channel(Control {
            generation: 0,
            admission: Admission::Open,
            to_server: Flow::Open,
            to_client: Flow::Open,
        });
        let accepted = Arc::new(AtomicU64::new(0));
        let accept = tokio::spawn(accept(
            listener,
            upstream.clone(),
            control.clone(),
            accepted.clone(),
        ));

        Ok(Self {
            addr,
            upstream,
            control,
            accepted,
            accept,
        })
    }

    /// Closes every new connection as soon as it arrives, before it reaches the server, until
    /// [`Proxy::admit`]. Connections already open are kept. The client sees the connection end during its
    /// handshake.
    pub fn refuse(&self) {
        self.control
            .send_modify(|c| c.admission = Admission::Refused);
    }

    /// Forwards new connections again after [`Proxy::refuse`].
    pub fn admit(&self) {
        self.control.send_modify(|c| c.admission = Admission::Open);
    }

    /// How many connections arrived at the proxy since it started, refused ones included.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }

    /// The address the proxy listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A URL for `db` through the proxy, with the user of the server URL.
    pub fn url(&self, db: &TestDb) -> Result<String> {
        let parts = crate::url_parts(&db.url)?;
        let Some((_, authority)) = parts.prefix.split_once("://") else {
            bail!("not a postgres URL: {}", db.url)
        };
        let user = authority
            .rsplit_once('@')
            .map(|(user, _)| format!("{user}@"))
            .unwrap_or_default();
        let query: Vec<&str> = parts
            .query
            .trim_start_matches('?')
            .split('&')
            .filter(|p| !p.is_empty() && !p.starts_with("host=") && !p.starts_with("port="))
            .collect();
        let query = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };

        Ok(format!(
            "postgres://{user}{}{}{query}",
            self.addr, parts.path
        ))
    }

    /// Closes every connection open through the proxy now, without a PostgreSQL goodbye. Bytes held for them
    /// are lost. New connections are accepted as before.
    pub fn drop_connections(&self) {
        self.control.send_modify(|c| c.generation += 1);
    }

    /// Stops forwarding bytes going `direction`, on open and new connections, until [`Proxy::release`].
    pub fn hold(&self, direction: Direction) {
        self.control.send_modify(|c| match direction {
            Direction::ToServer => c.to_server = Flow::Held,
            Direction::ToClient => c.to_client = Flow::Held,
            Direction::Both => {
                c.to_server = Flow::Held;
                c.to_client = Flow::Held;
            }
        });
    }

    /// Forwards traffic both ways again, including what was held.
    pub fn release(&self) {
        self.control.send_modify(|c| {
            c.to_server = Flow::Open;
            c.to_client = Flow::Open;
        });
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.drop_connections();
    }
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("addr", &self.addr)
            .field("upstream", &self.upstream)
            .finish_non_exhaustive()
    }
}

/// Which way the bytes [`Proxy::hold`] stops go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From the client to the server: statements do not reach the server.
    ToServer,
    /// From the server to the client: statements run, their responses wait.
    ToClient,
    /// Both ways.
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Control {
    /// Bumped by [`Proxy::drop_connections`]; a connection ends when it differs from the one it started with.
    generation: u64,
    admission: Admission,
    to_server: Flow,
    to_client: Flow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    Open,
    Refused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Open,
    Held,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Upstream {
    Tcp(String),
    Unix(PathBuf),
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

impl Upstream {
    /// Where the server of `url` listens: the `host` query parameter (a socket directory when it starts with
    /// `/`) or the URL's host, and the `port` parameter or the URL's port.
    fn of(url: &str) -> Result<Self> {
        let parts = crate::url_parts(url)?;
        let Some((_, authority)) = parts.prefix.split_once("://") else {
            bail!("not a postgres URL: {url}")
        };
        let host_port = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let (url_host, url_port) = match host_port.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (host_port, None),
        };
        let param = |name: &str| {
            parts
                .query
                .trim_start_matches('?')
                .split('&')
                .find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
                .map(percent_decode)
        };
        let port = param("port")
            .or(url_port.map(str::to_owned))
            .unwrap_or_else(|| "5432".to_owned());

        match param("host") {
            Some(dir) if dir.starts_with('/') => Ok(Self::Unix(
                PathBuf::from(dir).join(format!(".s.PGSQL.{port}")),
            )),
            Some(host) => Ok(Self::Tcp(format!("{host}:{port}"))),
            None if url_host.is_empty() => Ok(Self::Tcp(format!("localhost:{port}"))),
            None => Ok(Self::Tcp(format!("{url_host}:{port}"))),
        }
    }

    async fn connect(&self) -> io::Result<Box<dyn Stream>> {
        Ok(match self {
            Self::Tcp(addr) => Box::new(TcpStream::connect(addr).await?),
            Self::Unix(path) => Box::new(UnixStream::connect(path).await?),
        })
    }
}

async fn accept(
    listener: TcpListener,
    upstream: Upstream,
    control: watch::Sender<Control>,
    accepted: Arc<AtomicU64>,
) {
    while let Ok((client, _)) = listener.accept().await {
        accepted.fetch_add(1, Ordering::SeqCst);
        let upstream = upstream.clone();
        let control = control.subscribe();
        // Read before connecting upstream: a drop_connections() after the accept must close this one too.
        let Control {
            generation,
            admission,
            ..
        } = *control.borrow();
        if admission == Admission::Refused {
            drop(client);
            continue;
        }
        tokio::spawn(async move {
            let Ok(server) = upstream.connect().await else {
                return;
            };
            let (client_read, client_write) = tokio::io::split(client);
            let (server_read, server_write) = tokio::io::split(server);

            // When either direction ends, both sockets are dropped and the connection closes.
            tokio::select! {
                _ = pump(client_read, server_write, Direction::ToServer, control.clone(), generation) => {}
                _ = pump(server_read, client_write, Direction::ToClient, control, generation) => {}
            }
        });
    }
}

/// Copies `from` to `to` while the connection's generation is current, waiting while `direction` is held.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    direction: Direction,
    mut control: watch::Receiver<Control>,
    generation: u64,
) -> io::Result<()> {
    let mut buf = vec![0; 16 * 1024];

    loop {
        let n = tokio::select! {
            read = from.read(&mut buf) => read?,
            () = dropped(&mut control, generation) => return Ok(()),
        };
        if n == 0 {
            return Ok(());
        }

        loop {
            let now = *control.borrow_and_update();
            if now.generation != generation {
                return Ok(());
            }
            let flow = match direction {
                Direction::ToServer => now.to_server,
                Direction::ToClient | Direction::Both => now.to_client,
            };
            if flow == Flow::Open {
                break;
            }
            if control.changed().await.is_err() {
                return Ok(());
            }
        }
        to.write_all(&buf[..n]).await?;
    }
}

/// Resolves once the connection's generation is no longer current, or the proxy is gone.
async fn dropped(control: &mut watch::Receiver<Control>, generation: u64) {
    loop {
        if control.borrow_and_update().generation != generation {
            return;
        }
        if control.changed().await.is_err() {
            return;
        }
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }

    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_upstream_of_server_urls() {
        assert_eq!(
            Upstream::of("postgres://postgres@localhost/postgres?host=/tmp/x/.pg&port=54318")
                .unwrap(),
            Upstream::Unix(PathBuf::from("/tmp/x/.pg/.s.PGSQL.54318"))
        );
        assert_eq!(
            Upstream::of("postgres://postgres@localhost/postgres?host=%2Ftmp%2Fy&port=5433")
                .unwrap(),
            Upstream::Unix(PathBuf::from("/tmp/y/.s.PGSQL.5433"))
        );
        assert_eq!(
            Upstream::of("postgres://p:p@127.0.0.1:32768/postgres").unwrap(),
            Upstream::Tcp("127.0.0.1:32768".to_owned())
        );
        assert_eq!(
            Upstream::of("postgres://p@db/postgres").unwrap(),
            Upstream::Tcp("db:5432".to_owned())
        );
    }
}
