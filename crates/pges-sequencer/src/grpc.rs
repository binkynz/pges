//! The gRPC watch server, `pges.watch.v1.Watch` (feature `watch-grpc`): each store's high-water mark,
//! streamed to subscribers in other processes from the sequencer's in-process [`watch::Watch`].
//!
//! Only the leader serves `Watch`. A sequencer that stands by, recovers outside a term, halted, stopped or
//! was abandoned answers `UNAVAILABLE`, so a client given every sequencer's address finds the leader by
//! trying each. A stream ends with `UNAVAILABLE` when its sequencer stops leading, and when the term's lease
//! passes without renewal while the status still says `Leading` (sequencing hangs on a database call, and
//! another sequencer may take over); new streams are then refused the same way. Each value carries the
//! term's fence, so a client can tell leaders apart; positions never go down within a term, and a new leader
//! starts from the log, so they never go down across terms either.
//!
//! A server serves at most [`WatchConfig::max_streams`] streams at once and refuses more with
//! `RESOURCE_EXHAUSTED`. A stream for a store no leader announced streams zeros, as the in-process watch
//! does, and its channel lasts only while some stream or receiver watches it.
//!
//! With `watch-grpc-tls`, [`WatchService::serve`] can serve over TLS, and require client certificates
//! signed by a given CA (`ServerTls`).
//!
//! The proto file is `pges-watch-proto`'s `proto/pges/watch/v1/watch.proto`.

use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream};
use tokio::time::Instant;

use pges_core::{high_water, non_empty, store};
use pges_watch_proto::v1;

use crate::{sequencer, watch};

/// Why a [`ServerTls`] could not be built: the same type as [`pges_core::tls::TlsError`].
#[cfg(feature = "watch-grpc-tls")]
pub use pges_core::tls::TlsError;

/// Serves `pges.watch.v1.Watch` from a sequencer's [`watch::Watch`].
///
/// [`WatchService::serve`] runs a server of its own on a listener. To add the service to a tonic server
/// that also serves other things, use [`WatchService::into_server`] instead.
///
/// ```no_run
/// # async fn example(
/// #     pool: sqlx::PgPool,
/// #     shutdown: impl std::future::Future<Output = ()> + Send,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// use pges_sequencer::{grpc, outbox, sequencer};
///
/// let source = outbox::OutboxSource::new(pool.clone(), outbox::OutboxConfig::default());
/// let sequencer = sequencer::Sequencer::new(pool, source, sequencer::SequencerConfig::default());
/// let listener = tokio::net::TcpListener::bind("0.0.0.0:50051").await?;
/// let (stop_server, server_stopped) = tokio::sync::oneshot::channel::<()>();
/// let server = tokio::spawn(grpc::WatchService::new(sequencer.watch()).serve(listener, async {
///     let _ = server_stopped.await;
/// }));
///
/// // Stop the server after the sequencer, so subscribers' streams end with its last values.
/// sequencer.run(shutdown).await?;
/// let _ = stop_server.send(());
/// server.await??;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WatchService {
    watch: watch::Watch,
    config: WatchConfig,
    streams: Arc<tokio::sync::Semaphore>,
    serving: tokio::sync::watch::Sender<Serving>,
}

impl WatchService {
    /// Serves the values `watch` announces, for example `sequencer.watch()`, with the default
    /// [`WatchConfig`].
    pub fn new(watch: watch::Watch) -> Self {
        Self::with_config(watch, WatchConfig::default())
    }

    /// Like [`WatchService::new`], with `config`.
    pub fn with_config(watch: watch::Watch, config: WatchConfig) -> Self {
        let streams = config
            .max_streams
            .get()
            .min(tokio::sync::Semaphore::MAX_PERMITS);

        Self {
            watch,
            config,
            streams: Arc::new(tokio::sync::Semaphore::new(streams)),
            serving: tokio::sync::watch::Sender::new(Serving::Open),
        }
    }

    /// Serves on `listener` until `shutdown` resolves.
    ///
    /// On shutdown it stops accepting connections, ends every open stream with `UNAVAILABLE` and returns
    /// once the connections have closed. HTTP/2 keepalive pings every 10 s close connections whose peer
    /// stopped answering within 20 s.
    ///
    /// Fails with [`ServeError`] when the server cannot accept connections.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ServeError> {
        self.serve_all(non_empty::NonEmpty::new(listener, []), shutdown)
            .await
    }

    /// Like [`WatchService::serve`], on every listener of `listeners` at once, for example one for each
    /// address a host name resolves to.
    pub async fn serve_all(
        self,
        listeners: non_empty::NonEmpty<tokio::net::TcpListener>,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ServeError> {
        let serving = self.serving.clone();
        let incoming = stream::select_all(listeners.into_vec().into_iter().map(|listener| {
            Box::pin(tonic::transport::server::TcpIncoming::from(listener).with_nodelay(Some(true)))
        }));

        let builder = tonic::transport::Server::builder();
        #[cfg(feature = "watch-grpc-tls")]
        let builder = match &self.config.tls {
            Some(tls) => builder
                .tls_config(tls.config.clone())
                .map_err(|error| ServeError(Box::new(error)))?,
            None => builder,
        };
        builder
            .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
            .serve_with_incoming_shutdown(self.into_server(), incoming, async move {
                shutdown.await;
                // Open streams end only when told to, and the server waits for them before it returns.
                serving.send_replace(Serving::Closing);
            })
            .await
            .map_err(|error| ServeError(Box::new(error)))
    }

    /// The service for a tonic server of the caller's own, for example one that serves other services too:
    /// `tonic::transport::Server::builder().add_service(service.into_server())`. Its streams end when the
    /// sequencer stops leading; a server shut down while the sequencer still leads waits for them, so stop
    /// the sequencer first or bound the server's shutdown. The caller's server sets up its own TLS; the
    /// config's TLS applies to [`WatchService::serve`] only.
    pub fn into_server(self) -> v1::watch_server::WatchServer<Self> {
        v1::watch_server::WatchServer::new(self)
    }
}

/// Settings of a [`WatchService`]. Start from [`WatchConfig::default`] and change fields.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WatchConfig {
    /// Most `Watch` streams open at once, over all connections. One more is refused with
    /// `RESOURCE_EXHAUSTED`, which a `GrpcWaker` retries like `UNAVAILABLE`. Each subscription with a gRPC
    /// waker holds one stream (one per shard instance). Default 10 000.
    pub max_streams: NonZeroUsize,
    /// TLS for [`WatchService::serve`]. Default none: plain HTTP/2.
    #[cfg(feature = "watch-grpc-tls")]
    pub tls: Option<ServerTls>,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            max_streams: NonZeroUsize::new(10_000).unwrap_or(NonZeroUsize::MIN),
            #[cfg(feature = "watch-grpc-tls")]
            tls: None,
        }
    }
}

/// The server's certificate and key, and optionally a CA that must have signed every client's certificate
/// (feature `watch-grpc-tls`; rustls with the ring provider). All PEM.
///
/// ```
/// # fn example(cert: &[u8], key: &[u8], clients_ca: &[u8]) -> Result<(), pges_sequencer::grpc::TlsError> {
/// use pges_sequencer::grpc;
///
/// let mut config = grpc::WatchConfig::default();
/// config.tls = Some(grpc::ServerTls::new(cert, key)?.client_ca(clients_ca)?);
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "watch-grpc-tls")]
#[derive(Debug, Clone)]
pub struct ServerTls {
    config: tonic::transport::ServerTlsConfig,
}

#[cfg(feature = "watch-grpc-tls")]
impl ServerTls {
    /// Serves with the certificate chain `cert` and its private key `key`, both PEM. Clients need no
    /// certificate.
    ///
    /// Fails with [`TlsError`] when they cannot be used: no certificate or no private key, more than one
    /// key, unreadable PEM, an invalid certificate, or a key that does not match it.
    pub fn new(cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<Self, TlsError> {
        certificates(cert.as_ref())?;
        private_key(key.as_ref())?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let config = tonic::transport::ServerTlsConfig::new()
            .identity(identity)
            .timeout(TLS_HANDSHAKE_TIMEOUT);

        Self::checked(config)
    }

    /// Also requires every client to present a certificate signed by `ca` (PEM): mutual TLS. A client
    /// without one, or with one signed by another CA, cannot connect.
    ///
    /// Fails with [`TlsError`] when `ca` holds no certificate, unreadable PEM, or a certificate that
    /// cannot be a trust anchor.
    pub fn client_ca(self, ca: impl AsRef<[u8]>) -> Result<Self, TlsError> {
        ca_certificates(ca.as_ref())?;
        let config = self
            .config
            .client_ca_root(tonic::transport::Certificate::from_pem(ca));

        Self::checked(config)
    }

    /// Builds the rustls configuration once, so a bad certificate or key fails here, not when serving.
    fn checked(config: tonic::transport::ServerTlsConfig) -> Result<Self, TlsError> {
        tonic::transport::Server::builder()
            .tls_config(config.clone())
            .map_err(|error| refused(&error))?;

        Ok(Self { config })
    }
}

/// The gRPC server stopped accepting connections, for example because the listener failed.
#[derive(Debug, thiserror::Error)]
#[error("the gRPC watch server failed: {0}")]
pub struct ServeError(Box<dyn std::error::Error + Send + Sync + 'static>);

#[tonic::async_trait]
impl v1::watch_server::Watch for WatchService {
    type WatchStream = stream::BoxStream<'static, Result<v1::WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<v1::WatchRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let store = store::StoreId::new(request.into_inner().store_id);
        let mut status = self.watch.status();
        let fence = match &*status.borrow_and_update() {
            sequencer::Status::Leading(fence) => *fence,
            other => return Err(not_leading(other)),
        };
        let mut lease = self.watch.lease_deadline();
        let expires = *lease.borrow_and_update();
        if expires.is_some_and(|expires| expires <= Instant::now()) {
            return Err(lease_expired());
        }
        let mut serving = self.serving.subscribe();
        if *serving.borrow_and_update() == Serving::Closing {
            return Err(tonic::Status::unavailable("the server is shutting down"));
        }
        let Ok(slot) = self.streams.clone().try_acquire_owned() else {
            return Err(tonic::Status::resource_exhausted(format!(
                "this server already serves its maximum of {} watch streams",
                self.config.max_streams
            )));
        };

        let streaming = Streaming {
            fence,
            high_water: self.watch.high_water(store),
            status,
            lease,
            expiry: expires.map(|expires| Box::pin(tokio::time::sleep_until(expires))),
            serving,
            next: Next::Current,
            _slot: slot,
        };

        Ok(tonic::Response::new(
            stream::unfold(streaming, Streaming::next).boxed(),
        ))
    }

    async fn status(
        &self,
        _request: tonic::Request<v1::StatusRequest>,
    ) -> Result<tonic::Response<v1::StatusResponse>, tonic::Status> {
        let (state, fence) = match &*self.watch.status().borrow() {
            sequencer::Status::Standby => (v1::SequencerState::Standby, None),
            sequencer::Status::Leading(fence) => (v1::SequencerState::Leading, Some(fence.get())),
            sequencer::Status::Recovering(_) => (v1::SequencerState::Recovering, None),
            sequencer::Status::Halted(_) => (v1::SequencerState::Halted, None),
            sequencer::Status::Stopped => (v1::SequencerState::Stopped, None),
            sequencer::Status::Abandoned => (v1::SequencerState::Abandoned, None),
        };

        Ok(tonic::Response::new(v1::StatusResponse {
            state: state.into(),
            fence,
        }))
    }
}

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// How long a client may take to finish the TLS handshake.
#[cfg(feature = "watch-grpc-tls")]
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether [`WatchService::serve`] is shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Serving {
    Open,
    Closing,
}

/// One client's stream.
struct Streaming {
    fence: sequencer::Fence,
    high_water: tokio::sync::watch::Receiver<high_water::HighWater>,
    status: tokio::sync::watch::Receiver<sequencer::Status>,
    lease: tokio::sync::watch::Receiver<Option<Instant>>,
    /// Fires when the term's lease expires unless renewed. One timer for the stream's life, moved by each
    /// renewal, rather than a new one for every value sent.
    expiry: Option<Pin<Box<tokio::time::Sleep>>>,
    serving: tokio::sync::watch::Receiver<Serving>,
    next: Next,
    /// One of [`WatchConfig::max_streams`], given back when the stream is dropped.
    _slot: tokio::sync::OwnedSemaphorePermit,
}

/// What the stream sends next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// The current value, without waiting.
    Current,
    /// The next change.
    Change,
    /// Nothing: the stream has ended.
    End,
}

/// What woke a waiting stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    HighWater,
    Status,
    Renewed,
    Expired,
    Gone,
    Closing,
}

impl Streaming {
    async fn next(mut self) -> Option<(Result<v1::WatchResponse, tonic::Status>, Self)> {
        loop {
            match self.next {
                Next::End => return None,
                Next::Current => {
                    self.next = Next::Change;
                    let message = self.current();
                    return Some((Ok(message), self));
                }
                Next::Change => {}
            }

            let event = tokio::select! {
                changed = self.high_water.changed() => match changed {
                    Ok(()) => Event::HighWater,
                    Err(_) => Event::Gone,
                },
                changed = self.status.changed() => match changed {
                    Ok(()) => Event::Status,
                    Err(_) => Event::Gone,
                },
                changed = self.lease.changed() => match changed {
                    Ok(()) => Event::Renewed,
                    Err(_) => Event::Gone,
                },
                () = expired(&mut self.expiry) => Event::Expired,
                _ = self.serving.wait_for(|serving| *serving == Serving::Closing) => Event::Closing,
            };

            match event {
                Event::HighWater => self.next = Next::Current,
                Event::Renewed => self.renewed(),
                // A leader whose sequencing hangs still says Leading, but another may have taken over.
                Event::Expired => {
                    let expires = *self.lease.borrow_and_update();
                    if expires.is_none_or(|expires| expires <= Instant::now()) {
                        return self.end(lease_expired());
                    }
                    self.renewed();
                }
                Event::Status => {
                    let leading = match &*self.status.borrow_and_update() {
                        sequencer::Status::Leading(fence) => Ok(*fence),
                        // Still in its term if it renews the lease in time; it says Standby if not.
                        sequencer::Status::Recovering(_) => continue,
                        other => Err(not_leading(other)),
                    };
                    let fence = match leading {
                        Ok(fence) => fence,
                        Err(status) => return self.end(status),
                    };
                    if fence != self.fence {
                        self.fence = fence;
                        self.next = Next::Current;
                    }
                }
                Event::Gone => {
                    return self.end(tonic::Status::unavailable("the sequencer is gone"));
                }
                Event::Closing => {
                    return self.end(tonic::Status::unavailable("the server is shutting down"));
                }
            }
        }
    }

    /// Moves the expiry timer to the lease's new deadline.
    fn renewed(&mut self) {
        let Some(expires) = *self.lease.borrow_and_update() else {
            return;
        };

        match &mut self.expiry {
            Some(expiry) => expiry.as_mut().reset(expires),
            None => self.expiry = Some(Box::pin(tokio::time::sleep_until(expires))),
        }
    }

    fn current(&mut self) -> v1::WatchResponse {
        let high_water = self.high_water.borrow_and_update();

        v1::WatchResponse::new(self.fence.get(), &high_water)
    }

    fn end(
        mut self,
        status: tonic::Status,
    ) -> Option<(Result<v1::WatchResponse, tonic::Status>, Self)> {
        self.next = Next::End;

        Some((Err(status), self))
    }
}

fn not_leading(status: &sequencer::Status) -> tonic::Status {
    let state = match status {
        sequencer::Status::Standby => "standing by",
        sequencer::Status::Leading(_) => "leading",
        sequencer::Status::Recovering(_) => "recovering",
        sequencer::Status::Halted(_) => "halted",
        sequencer::Status::Stopped => "stopped",
        sequencer::Status::Abandoned => "abandoned",
    };

    tonic::Status::unavailable(format!("this sequencer is not leading: {state}"))
}

fn lease_expired() -> tonic::Status {
    tonic::Status::unavailable(
        "this sequencer's lease expired without renewal: it is stalled, and another may lead",
    )
}

/// Waits for `expiry`, or forever without one.
async fn expired(expiry: &mut Option<Pin<Box<tokio::time::Sleep>>>) {
    match expiry {
        Some(expiry) => expiry.as_mut().await,
        None => std::future::pending().await,
    }
}

/// Checks that `pem` holds certificates, and only readable ones: tonic accepts text with none.
#[cfg(feature = "watch-grpc-tls")]
fn certificates(pem: &[u8]) -> Result<(), TlsError> {
    match pem_certificates(pem) {
        Ok(certificates) if certificates.is_empty() => Err(TlsError::NoCertificate),
        Ok(_) => Ok(()),
        Err(error) => Err(TlsError::UnreadableCertificate {
            reason: error.to_string(),
        }),
    }
}

/// Checks that `pem` holds certificates that can all be trust anchors: tonic skips those that cannot.
#[cfg(feature = "watch-grpc-tls")]
fn ca_certificates(pem: &[u8]) -> Result<(), TlsError> {
    let certificates =
        pem_certificates(pem).map_err(|error| TlsError::UnreadableCaCertificate {
            reason: error.to_string(),
        })?;
    if certificates.is_empty() {
        return Err(TlsError::NoCaCertificate);
    }

    let mut roots = rustls::RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| TlsError::InvalidCaCertificate {
                reason: error.to_string(),
            })?;
    }

    Ok(())
}

/// Checks that `pem` holds exactly one private key: tonic takes the first and ignores the rest.
#[cfg(feature = "watch-grpc-tls")]
fn private_key(pem: &[u8]) -> Result<(), TlsError> {
    use rustls_pki_types::pem::PemObject as _;

    let keys = rustls_pki_types::PrivateKeyDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| TlsError::UnreadablePrivateKey {
            reason: error.to_string(),
        })?;

    match keys.len() {
        0 => Err(TlsError::NoPrivateKey),
        1 => Ok(()),
        count => Err(TlsError::MultiplePrivateKeys { count }),
    }
}

/// The certificates in `pem`, skipping its other sections.
#[cfg(feature = "watch-grpc-tls")]
fn pem_certificates(
    pem: &[u8],
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, rustls_pki_types::pem::Error> {
    use rustls_pki_types::pem::PemObject as _;

    rustls_pki_types::CertificateDer::pem_slice_iter(pem).collect()
}

/// Why tonic refused the settings, from the rustls error it wraps. The PEM was checked before, so what is
/// left is the key and the certificate themselves.
#[cfg(feature = "watch-grpc-tls")]
fn refused(error: &tonic::transport::Error) -> TlsError {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        match cause.downcast_ref::<rustls::Error>() {
            Some(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::KeyMismatch)) => {
                return TlsError::KeyMismatch;
            }
            Some(tls @ rustls::Error::InvalidCertificate(_)) => {
                return TlsError::InvalidCertificate {
                    reason: tls.to_string(),
                };
            }
            Some(_) => break,
            None => source = cause.source(),
        }
    }

    TlsError::Rejected {
        reason: chain(error),
    }
}

/// An error with its sources, `: `-separated: tonic's transport errors say little on their own.
#[cfg(feature = "watch-grpc-tls")]
fn chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.ends_with(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }

    text
}

#[cfg(all(test, feature = "watch-grpc-tls"))]
mod tests {
    use super::*;

    /// A PEM section that never ends.
    const UNTERMINATED: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n";
    /// A certificate whose DER is three zero bytes.
    const GARBAGE_CERTIFICATE: &str =
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    /// A PKCS#8 private key whose DER is three zero bytes.
    const GARBAGE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";

    #[test]
    fn valid_tls_files_are_accepted() {
        let pki = pges_testing::tls::Pki::generate().unwrap();

        let tls = ServerTls::new(&pki.server_cert, &pki.server_key).unwrap();
        assert!(tls.client_ca(&pki.ca).is_ok());
    }

    #[test]
    fn each_unusable_server_identity_is_refused_up_front_with_its_cause() {
        let pki = pges_testing::tls::Pki::generate().unwrap();
        let refused = |cert: &str, key: &str| ServerTls::new(cert, key).unwrap_err();

        assert_eq!(
            refused("not a certificate", &pki.server_key),
            TlsError::NoCertificate
        );
        assert!(matches!(
            refused(UNTERMINATED, &pki.server_key),
            TlsError::UnreadableCertificate { .. }
        ));
        assert!(matches!(
            refused(GARBAGE_CERTIFICATE, &pki.server_key),
            TlsError::InvalidCertificate { .. }
        ));
        assert_eq!(
            refused(&pki.server_cert, &pki.server_cert),
            TlsError::NoPrivateKey
        );
        assert!(matches!(
            refused(&pki.server_cert, "-----BEGIN PRIVATE KEY-----\nAAAA\n"),
            TlsError::UnreadablePrivateKey { .. }
        ));
        assert_eq!(
            refused(
                &pki.server_cert,
                &format!("{}{}", pki.server_key, pki.client_key)
            ),
            TlsError::MultiplePrivateKeys { count: 2 }
        );
        assert_eq!(
            refused(&pki.server_cert, &pki.client_key),
            TlsError::KeyMismatch
        );
        assert!(matches!(
            refused(&pki.server_cert, GARBAGE_KEY),
            TlsError::Rejected { .. }
        ));
    }

    #[test]
    fn each_unusable_client_ca_is_refused_up_front_with_its_cause() {
        let pki = pges_testing::tls::Pki::generate().unwrap();
        let tls = ServerTls::new(&pki.server_cert, &pki.server_key).unwrap();
        let refused = |ca: &str| tls.clone().client_ca(ca).unwrap_err();

        assert_eq!(refused("not a certificate"), TlsError::NoCaCertificate);
        assert_eq!(refused(&pki.server_key), TlsError::NoCaCertificate);
        assert!(matches!(
            refused(UNTERMINATED),
            TlsError::UnreadableCaCertificate { .. }
        ));
        assert!(matches!(
            refused(&format!("{}{GARBAGE_CERTIFICATE}", pki.ca)),
            TlsError::InvalidCaCertificate { .. }
        ));
    }
}
