//! Waking from the sequencer's gRPC watch, `pges.watch.v1.Watch`: the production default, for subscribers in
//! other processes than the sequencer.

use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use tokio::time::Instant;

use pges_core::{category, high_water, interval, lsn, non_empty, position, store};
use pges_watch_proto::v1;

use crate::waker;

/// Why a [`ClientTls`] could not be built: the same type as [`pges_core::tls::TlsError`].
#[cfg(feature = "wake-grpc-tls")]
pub use pges_core::tls::TlsError;

/// Streams a store's high-water mark from the leading sequencer's gRPC watch (its `watch-grpc` feature,
/// `pges-sequencer --grpc-listen`).
///
/// Give it the address of every sequencer of the database. Only the leader serves the watch; the others answer
/// `UNAVAILABLE`, so the waker tries each address in turn until one serves, and moves on when the leader stops
/// leading. Values from every leader are merged, keeping the highest position of the store and of each
/// category, and the highest LSN: positions never go down, so the waker never reports less than it already
/// did.
///
/// It resolves when the highest values seen so far have passed `known` ([`waker::has_passed`]), at once if
/// they already have, even while no stream is open. It also resolves once, whatever `known` says, on the first
/// value of each new stream (the first stream and every reconnect), because values sent while no stream was
/// open are gone, and on a value from a leader with a higher fence than any before.
///
/// While no endpoint serves, the waker keeps trying inside [`Waker::changed`](waker::Waker::changed): after a
/// round in which every endpoint failed it waits 100 ms, doubling after each further failed round up to 5 s.
/// A stream that ended within 5 s of opening counts as a failed round; one that lasted longer is replaced at
/// once. Nothing new is sequenced while no sequencer leads, so a short gap (a restart, a handover) costs the
/// subscriber nothing. Once no endpoint has served for [`GrpcConfig::unreachable_after`], each failed round
/// fails the call with [`WakeError::Other`](waker::WakeError::Other) holding an [`Unreachable`]; a
/// subscription then shows `Recovering` and reads the log after each retry until an endpoint serves again.
///
/// Refusals that retrying does not fix are reported at once, as a [`Rejected`] in the same place: a round in
/// which no endpoint served and one answered `UNIMPLEMENTED`, `INVALID_ARGUMENT`, `PERMISSION_DENIED`,
/// `UNAUTHENTICATED` or `FAILED_PRECONDITION` (a wrong server or port), or failed TLS on a certificate; and a
/// stream that sent a position no sequencer sends. Other failures, `UNAVAILABLE` and `RESOURCE_EXHAUSTED`
/// among them, are retried as above.
///
/// The waiting and the reconnects happen inside the call, so a subscriber that drops it (to shut down or renew
/// its lease) does not wait for them, and a later call waits only for what is left of the backoff. An attempt
/// cut short by a dropped call counts as failed, and the next call tries the next endpoint. HTTP/2
/// keepalive pings detect a connection that died silently. The waker keeps no connection of the store's pool.
///
/// Endpoints are `http://host:port` URIs ([`Endpoint`]), or `https://host:port` with the `wake-grpc-tls`
/// feature, verified against the system's root certificates or a given CA (`GrpcConfig::tls`).
///
/// ```no_run
/// use pges_core::non_empty;
/// use pges_subscribe::waker::grpc;
///
/// # fn example(store: pges_core::store::StoreId) -> Result<(), grpc::EndpointError> {
/// let waker = grpc::GrpcWaker::new(
///     store,
///     non_empty!["http://sequencer-0:50051".parse()?, "http://sequencer-1:50051".parse()?],
/// );
/// # Ok(())
/// # }
/// ```
pub struct GrpcWaker {
    store: store::StoreId,
    endpoints: Vec<Target>,
    config: GrpcConfig,
    next_endpoint: usize,
    watching: Option<Watching>,
    seen: Seen,
    recheck: Recheck,
    backoff: Backoff,
    round: Round,
    unreachable_since: Option<Instant>,
}

impl GrpcWaker {
    /// Watches `store` through the sequencers at `endpoints`, with the default [`GrpcConfig`]. Connects on
    /// the first call, not here.
    pub fn new(store: store::StoreId, endpoints: non_empty::NonEmpty<Endpoint>) -> Self {
        Self::with_config(store, endpoints, GrpcConfig::default())
    }

    /// Like [`GrpcWaker::new`], with `config`.
    pub fn with_config(
        store: store::StoreId,
        endpoints: non_empty::NonEmpty<Endpoint>,
        config: GrpcConfig,
    ) -> Self {
        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.configured(&config))
            .collect();

        Self {
            store,
            endpoints,
            config,
            next_endpoint: 0,
            watching: None,
            seen: Seen::default(),
            recheck: Recheck::NotDue,
            backoff: Backoff::default(),
            round: Round::default(),
            unreachable_since: None,
        }
    }

    /// Opens a stream on the first endpoint that serves one, starting with the one that served last. Fails
    /// once every endpoint has failed since the last stream opened: with a [`Rejected`] if an endpoint
    /// refused in a way retrying does not fix, otherwise with the last failure.
    ///
    /// Every attempt is recorded before it is awaited, and a dropped attempt counts as failed: a round of
    /// slow endpoints ends even when the subscriber drops each call before an attempt completes.
    async fn connect(&mut self) -> Result<Watching, RoundFailed> {
        while self.round.failed < self.endpoints.len() {
            let index = self.next_endpoint % self.endpoints.len();
            self.next_endpoint = index + 1;
            self.round.failed += 1;
            self.round.last = format!("{}: the attempt was interrupted", self.endpoints[index].uri);
            self.unreachable_since.get_or_insert_with(Instant::now);

            // Bounds the HTTP/2 handshake and the response headers too, which the endpoint's own connect
            // timeout does not.
            let attempt =
                tokio::time::timeout(self.config.connect_timeout.get(), self.watch(index)).await;
            let attempt = attempt.unwrap_or_else(|_| {
                Err(Failed {
                    reason: "timed out".to_owned(),
                    rejection: None,
                })
            });
            match attempt {
                Ok(stream) => {
                    self.next_endpoint = index;
                    self.round = Round::default();
                    self.unreachable_since = None;

                    return Ok(Watching {
                        stream,
                        opened: Instant::now(),
                        first: First::Awaited,
                    });
                }
                Err(failed) => {
                    let endpoint = &self.endpoints[index].uri;
                    self.round.last = format!("{endpoint}: {}", failed.reason);
                    if let Some(rejection) = failed.rejection {
                        tracing::warn!(
                            store = self.store.get(),
                            endpoint,
                            reason = failed.reason,
                            "a gRPC watch endpoint refused the waker"
                        );
                        self.round.rejected = Some(Rejected {
                            store: self.store,
                            endpoint: endpoint.clone(),
                            rejection,
                            reason: failed.reason,
                        });
                    }
                }
            }
        }

        let round = std::mem::take(&mut self.round);
        Err(match round.rejected {
            Some(rejected) => RoundFailed::Rejected(rejected),
            None => RoundFailed::Unreachable(round.last),
        })
    }

    async fn watch(
        &self,
        index: usize,
    ) -> Result<tonic::codec::Streaming<v1::WatchResponse>, Failed> {
        let endpoint = match &self.endpoints[index].endpoint {
            Ok(endpoint) => endpoint,
            Err(reason) => {
                return Err(Failed {
                    reason: reason.clone(),
                    rejection: Some(Rejection::FailedPrecondition),
                });
            }
        };
        let channel = endpoint.connect().await.map_err(|error| Failed {
            reason: chain(&error),
            rejection: tls_rejection(&error),
        })?;
        let mut client = v1::watch_client::WatchClient::new(channel);
        let response = client
            .watch(v1::WatchRequest {
                store_id: self.store.get(),
            })
            .await
            .map_err(|status| Failed {
                reason: chain(&status),
                rejection: Rejection::of(&status),
            })?;

        Ok(response.into_inner())
    }
}

impl waker::Waker for GrpcWaker {
    /// Resolves with the highest positions any leader announced. Categories no leader listed are at
    /// [`position::Position::BEGINNING`].
    async fn changed(
        &mut self,
        known: &high_water::HighWater,
    ) -> Result<high_water::HighWater, waker::WakeError> {
        loop {
            // Checked before connecting: what earlier streams announced stays true while none is open.
            let high = self.seen.high_water();
            if self.recheck == Recheck::Due {
                self.recheck = Recheck::NotDue;
                return Ok(high);
            }
            if waker::has_passed(&high, known) {
                return Ok(high);
            }

            // Cancel safety: the stream, the values and the recheck are stored with no await in between, so
            // a call dropped after a stream's first value still fires on the next call. The backoff stores its
            // deadline, so a dropped call does not restart the wait.
            let watching = match &mut self.watching {
                Some(watching) => watching,
                None => {
                    if let Some(retry_at) = self.backoff.retry_at {
                        tokio::time::sleep_until(retry_at).await;
                    }
                    match self.connect().await {
                        Ok(watching) => {
                            self.backoff.connected();
                            self.watching.insert(watching)
                        }
                        Err(RoundFailed::Rejected(rejected)) => {
                            self.backoff.failed(Instant::now());
                            return Err(waker::WakeError::Other(Box::new(rejected)));
                        }
                        Err(RoundFailed::Unreachable(last)) => {
                            let now = Instant::now();
                            self.backoff.failed(now);
                            let since = *self.unreachable_since.get_or_insert(now);
                            let unreachable = now.saturating_duration_since(since);
                            if unreachable >= self.config.unreachable_after {
                                return Err(waker::WakeError::Other(Box::new(Unreachable {
                                    store: self.store,
                                    unreachable,
                                    last,
                                })));
                            }
                            continue;
                        }
                    }
                }
            };

            let received = match watching.stream.message().await {
                Ok(Some(message)) => Received::Value(self.seen.record(&message)),
                Ok(None) | Err(_) => Received::End,
            };
            match received {
                Received::Value(Some(term)) => {
                    // The first value of a new stream: values sent while no stream was open are gone.
                    if watching.first == First::Awaited || term == Term::New {
                        watching.first = First::Received;
                        self.recheck = Recheck::Due;
                    }
                }
                // A position no sequencer sends: the server is broken, and reconnecting will not fix it.
                // Values may have been missed, so the first value of the next stream fires.
                Received::Value(None) => {
                    self.watching = None;
                    self.backoff.failed(Instant::now());
                    let endpoint = self.endpoints[self.next_endpoint % self.endpoints.len()]
                        .uri
                        .clone();
                    return Err(waker::WakeError::Other(Box::new(Rejected {
                        store: self.store,
                        endpoint,
                        rejection: Rejection::Protocol,
                        reason: format!(
                            "it sent a position above {}",
                            position::Position::MAX.get()
                        ),
                    })));
                }
                // The stream ended: the server stopped leading, shut down, or the connection broke. Values
                // may have been missed: find the leader, then fire.
                Received::End => {
                    let opened = watching.opened;
                    self.watching = None;
                    self.backoff.lost(opened, Instant::now());
                }
            }
        }
    }
}

impl fmt::Debug for GrpcWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcWaker")
            .field("store", &self.store)
            .field(
                "endpoints",
                &self
                    .endpoints
                    .iter()
                    .map(|target| &target.uri)
                    .collect::<Vec<_>>(),
            )
            .field("watching", &self.watching.is_some())
            .finish_non_exhaustive()
    }
}

/// Settings of a [`GrpcWaker`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GrpcConfig {
    /// How long connecting to one endpoint and opening the stream may take. Default 5 s.
    pub connect_timeout: interval::Interval,
    /// How often an HTTP/2 ping checks an open connection. Default 10 s.
    pub keepalive_interval: interval::Interval,
    /// How long a ping may go unanswered before the connection counts as lost. Default 20 s.
    pub keepalive_timeout: interval::Interval,
    /// How long no endpoint may serve before [`Waker::changed`](waker::Waker::changed) fails with
    /// [`Unreachable`]. Longer than a sequencer handover: a crashed leader's lease must expire first
    /// (`lease_ttl`, 10 s by default). Default 30 s.
    pub unreachable_after: Duration,
    /// How `https` endpoints are verified, and the client certificate they are shown, if any. Default
    /// [`ClientTls::system_roots`]. Plain `http` endpoints ignore it.
    #[cfg(feature = "wake-grpc-tls")]
    pub tls: ClientTls,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            connect_timeout: pges_core::interval!(5 s),
            keepalive_interval: pges_core::interval!(10 s),
            keepalive_timeout: pges_core::interval!(20 s),
            unreachable_after: Duration::from_secs(30),
            #[cfg(feature = "wake-grpc-tls")]
            tls: ClientTls::system_roots(),
        }
    }
}

/// How a [`GrpcWaker`] verifies `https` endpoints (feature `wake-grpc-tls`; rustls with the ring provider):
/// the system's root certificates or one CA, and optionally a client certificate for servers that require
/// one (mutual TLS). The server's certificate must name the endpoint's host.
///
/// ```
/// # fn example(ca: &[u8], cert: &[u8], key: &[u8]) -> Result<(), pges_subscribe::waker::grpc::TlsError> {
/// use pges_subscribe::waker::grpc;
///
/// let mut config = grpc::GrpcConfig::default();
/// config.tls = grpc::ClientTls::ca(ca)?.identity(cert, key)?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "wake-grpc-tls")]
#[derive(Clone)]
pub struct ClientTls {
    config: tonic::transport::ClientTlsConfig,
}

#[cfg(feature = "wake-grpc-tls")]
impl ClientTls {
    /// Trusts the operating system's root certificates. If none can be loaded, every `https` endpoint fails
    /// with that reason.
    pub fn system_roots() -> Self {
        Self {
            config: tonic::transport::ClientTlsConfig::new()
                .with_native_roots()
                .timeout(TLS_HANDSHAKE_TIMEOUT),
        }
    }

    /// Trusts only `ca`, PEM: servers must present a certificate it signed.
    ///
    /// Fails with [`TlsError`] when `ca` holds no certificate, unreadable PEM, or a certificate that
    /// cannot be a trust anchor.
    pub fn ca(ca: impl AsRef<[u8]>) -> Result<Self, TlsError> {
        ca_certificates(ca.as_ref())?;

        Self::checked(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(ca))
                .timeout(TLS_HANDSHAKE_TIMEOUT),
        )
    }

    /// Presents the certificate chain `cert` with its private key `key`, both PEM, to servers that require
    /// client certificates.
    ///
    /// Fails with [`TlsError`] when they cannot be used: no certificate or no private key, more than one
    /// key, unreadable PEM, an invalid certificate, or a key that does not match it. With
    /// [`ClientTls::system_roots`], also when the system has no root certificates.
    pub fn identity(self, cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Result<Self, TlsError> {
        certificates(cert.as_ref())?;
        private_key(key.as_ref())?;

        Self::checked(
            self.config
                .identity(tonic::transport::Identity::from_pem(cert, key)),
        )
    }

    /// Builds the rustls configuration once, so bad certificates fail here, not on every connection attempt.
    fn checked(config: tonic::transport::ClientTlsConfig) -> Result<Self, TlsError> {
        tonic::transport::Endpoint::from_static("https://localhost")
            .tls_config(config.clone())
            .map_err(|error| refused(&error))?;

        Ok(Self { config })
    }
}

/// Checks that `pem` holds certificates, and only readable ones: tonic accepts text with none.
#[cfg(feature = "wake-grpc-tls")]
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
#[cfg(feature = "wake-grpc-tls")]
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
#[cfg(feature = "wake-grpc-tls")]
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
#[cfg(feature = "wake-grpc-tls")]
fn pem_certificates(
    pem: &[u8],
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>, rustls_pki_types::pem::Error> {
    use rustls_pki_types::pem::PemObject as _;

    rustls_pki_types::CertificateDer::pem_slice_iter(pem).collect()
}

/// Why tonic refused the settings, from the rustls error it wraps. The PEM was checked before, so what is
/// left is the key and the certificate themselves.
#[cfg(feature = "wake-grpc-tls")]
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

#[cfg(feature = "wake-grpc-tls")]
impl fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTls").finish_non_exhaustive()
    }
}

/// The address of a sequencer's gRPC watch: an `http://host:port` URI, or `https://host:port` with the
/// `wake-grpc-tls` feature, parsed from text with [`str::parse`].
///
/// ```
/// use pges_subscribe::waker::grpc;
///
/// let parsed: grpc::Endpoint = "http://sequencer-0:50051".parse()?;
///
/// assert_eq!(parsed.to_string(), "http://sequencer-0:50051/");
/// assert!("ftp://sequencer-0:50051".parse::<grpc::Endpoint>().is_err());
/// # Ok::<(), grpc::EndpointError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Endpoint(tonic::transport::Endpoint);

impl Endpoint {
    fn configured(self, config: &GrpcConfig) -> Target {
        let uri = self.0.uri().to_string();
        let endpoint = self
            .0
            .connect_timeout(config.connect_timeout.get())
            .http2_keep_alive_interval(config.keepalive_interval.get())
            .keep_alive_timeout(config.keepalive_timeout.get())
            .keep_alive_while_idle(true)
            .tcp_nodelay(true);
        #[cfg(feature = "wake-grpc-tls")]
        let endpoint = match endpoint.uri().scheme_str() {
            Some("https") => endpoint
                .tls_config(config.tls.config.clone())
                .map_err(|error| format!("TLS cannot be set up: {}", chain(&error))),
            _ => Ok(endpoint),
        };
        #[cfg(not(feature = "wake-grpc-tls"))]
        let endpoint = Ok(endpoint);

        Target { uri, endpoint }
    }
}

impl FromStr for Endpoint {
    type Err = EndpointError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let endpoint = match tonic::transport::Endpoint::from_shared(text.to_owned()) {
            Ok(endpoint) => endpoint,
            Err(_) => {
                return Err(EndpointError::Invalid {
                    endpoint: text.to_owned(),
                });
            }
        };
        let scheme = endpoint.uri().scheme_str();
        let supported = scheme == Some("http") || (TLS && scheme == Some("https"));
        if !supported || endpoint.uri().host().is_none() {
            return Err(EndpointError::UnsupportedScheme {
                endpoint: text.to_owned(),
            });
        }

        Ok(Self(endpoint))
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.uri().fmt(f)
    }
}

/// An [`Endpoint`] could not be parsed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EndpointError {
    /// The endpoint is not a valid URI.
    #[error("{endpoint:?} is not a valid endpoint URI")]
    Invalid {
        /// The endpoint as given.
        endpoint: String,
    },
    /// The endpoint's scheme is not `http`, or `https` with the `wake-grpc-tls` feature.
    #[error("{endpoint:?}: the scheme must be http, or https with the wake-grpc-tls feature")]
    UnsupportedScheme {
        /// The endpoint as given.
        endpoint: String,
    },
}

/// No endpoint of a [`GrpcWaker`] served the store's watch for [`GrpcConfig::unreachable_after`] or longer.
/// [`Waker::changed`](waker::Waker::changed) fails with it inside
/// [`WakeError::Other`](waker::WakeError::Other).
#[derive(Debug, thiserror::Error)]
#[error(
    "no gRPC watch endpoint served store {} for {unreachable:?}; the last error: {last}",
    store.get()
)]
pub struct Unreachable {
    store: store::StoreId,
    unreachable: Duration,
    last: String,
}

impl Unreachable {
    /// How long no endpoint has served.
    pub fn unreachable(&self) -> Duration {
        self.unreachable
    }
}

/// An endpoint of a [`GrpcWaker`] answered in a way that retrying does not fix: it is not a pges watch, it
/// refuses this client, or it sent a value no sequencer sends. [`Waker::changed`](waker::Waker::changed)
/// fails with it inside [`WakeError::Other`](waker::WakeError::Other) at once, instead of retrying silently
/// until [`GrpcConfig::unreachable_after`].
///
/// A refusal fails the call only when no endpoint of the round served: the other endpoints are tried first,
/// and a refusal that another endpoint makes up for is only logged (`tracing`, at warn). A value no sequencer
/// sends fails the call that received it. Either way the next call tries again after the backoff, so a
/// subscription shows `Recovering` with this error and reads the log on its retry interval until the
/// endpoint is fixed.
#[derive(Debug, thiserror::Error)]
#[error(
    "the gRPC watch endpoint {endpoint} cannot serve store {}: {reason}",
    store.get()
)]
pub struct Rejected {
    store: store::StoreId,
    endpoint: String,
    rejection: Rejection,
    reason: String,
}

impl Rejected {
    /// What the endpoint did.
    pub fn rejection(&self) -> Rejection {
        self.rejection
    }

    /// The endpoint, as a URI.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// How an endpoint refused a [`GrpcWaker`] ([`Rejected`]). Other failures (`UNAVAILABLE`, timeouts,
/// `RESOURCE_EXHAUSTED`, broken connections) are retried until [`GrpcConfig::unreachable_after`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Rejection {
    /// `UNIMPLEMENTED`: the server does not serve `pges.watch.v1.Watch`, for example another service on
    /// that port.
    Unimplemented,
    /// `INVALID_ARGUMENT`.
    InvalidArgument,
    /// `PERMISSION_DENIED`.
    PermissionDenied,
    /// `UNAUTHENTICATED`.
    Unauthenticated,
    /// `FAILED_PRECONDITION`, or an endpoint whose TLS could not be set up locally (for example no system
    /// root certificates).
    FailedPrecondition,
    /// The TLS handshake failed for a reason in the certificates: the server's is not trusted, has expired
    /// or does not name the host, or the server refused the client's. A server that requires a client
    /// certificate the client lacks may instead just close the connection, which is retried.
    Tls,
    /// The server sent a position above [`position::Position::MAX`], which no sequencer sends.
    Protocol,
}

impl Rejection {
    /// The rejection a gRPC status means, if retrying does not fix it.
    fn of(status: &tonic::Status) -> Option<Self> {
        match status.code() {
            tonic::Code::Unimplemented => Some(Self::Unimplemented),
            tonic::Code::InvalidArgument => Some(Self::InvalidArgument),
            tonic::Code::PermissionDenied => Some(Self::PermissionDenied),
            tonic::Code::Unauthenticated => Some(Self::Unauthenticated),
            tonic::Code::FailedPrecondition => Some(Self::FailedPrecondition),
            _ => tls_rejection(status),
        }
    }
}

/// Whether `https` endpoints are supported.
const TLS: bool = cfg!(feature = "wake-grpc-tls");

/// How long a TLS handshake may take.
#[cfg(feature = "wake-grpc-tls")]
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The first wait before trying again after a failed round.
const MIN_BACKOFF: Duration = Duration::from_millis(100);

/// The longest wait before trying again; also how long a stream must stay open to reset it.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// An endpoint, ready to connect, or why it cannot be.
struct Target {
    uri: String,
    endpoint: Result<tonic::transport::Endpoint, String>,
}

/// An error with its sources, `: `-separated: tonic's transport errors say little on their own.
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

/// An open stream.
struct Watching {
    stream: tonic::codec::Streaming<v1::WatchResponse>,
    opened: Instant,
    first: First,
}

/// The connection attempts since the last stream opened.
#[derive(Debug, Default)]
struct Round {
    failed: usize,
    last: String,
    /// The last refusal that retrying does not fix.
    rejected: Option<Rejected>,
}

/// Why a round ended without a stream.
enum RoundFailed {
    /// An endpoint refused in a way retrying does not fix.
    Rejected(Rejected),
    /// Every endpoint failed for reasons that may pass; the last one.
    Unreachable(String),
}

/// One attempt's failure.
struct Failed {
    reason: String,
    rejection: Option<Rejection>,
}

/// What a stream delivered: a value (`None` if it holds a position no sequencer sends), or its end.
enum Received {
    Value(Option<Term>),
    End,
}

/// [`Rejection::Tls`] if `error`, or an error it wraps, is a TLS failure caused by a certificate.
#[cfg(feature = "wake-grpc-tls")]
fn tls_rejection(error: &(dyn std::error::Error + 'static)) -> Option<Rejection> {
    let mut next = Some(error);
    while let Some(current) = next {
        if let Some(tls) = current.downcast_ref::<rustls::Error>() {
            return certificate_failure(tls).then_some(Rejection::Tls);
        }
        // `io::Error::source` skips the error it wraps, which is where rustls puts its own.
        if let Some(io) = current.downcast_ref::<std::io::Error>()
            && let Some(inner) = io.get_ref()
            && let Some(rejection) = tls_rejection(inner)
        {
            return Some(rejection);
        }
        next = current.source();
    }

    None
}

#[cfg(not(feature = "wake-grpc-tls"))]
fn tls_rejection(_error: &(dyn std::error::Error + 'static)) -> Option<Rejection> {
    None
}

/// Whether a TLS error comes from a certificate, which a retry does not change.
#[cfg(feature = "wake-grpc-tls")]
fn certificate_failure(error: &rustls::Error) -> bool {
    use rustls::AlertDescription as Alert;

    match error {
        rustls::Error::InvalidCertificate(_)
        | rustls::Error::NoCertificatesPresented
        | rustls::Error::PeerIncompatible(_) => true,
        rustls::Error::AlertReceived(alert) => matches!(
            alert,
            Alert::BadCertificate
                | Alert::UnsupportedCertificate
                | Alert::CertificateRevoked
                | Alert::CertificateExpired
                | Alert::CertificateUnknown
                | Alert::UnknownCA
                | Alert::AccessDenied
                | Alert::CertificateRequired
        ),
        _ => false,
    }
}

/// Whether a stream's first value has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum First {
    Awaited,
    Received,
}

/// Whether the next call resolves at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recheck {
    Due,
    NotDue,
}

/// Whether a value came from a leader with a higher fence than the one before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Term {
    New,
    Known,
}

/// When the waker may try to connect again.
#[derive(Debug, Default)]
struct Backoff {
    /// Failed rounds and short-lived streams since the last stream that stayed open.
    failures: u32,
    retry_at: Option<Instant>,
}

impl Backoff {
    fn failed(&mut self, now: Instant) {
        self.failures = self.failures.saturating_add(1);
        self.retry_at = Some(now + self.delay());
    }

    fn connected(&mut self) {
        self.retry_at = None;
    }

    fn lost(&mut self, opened: Instant, now: Instant) {
        if now.saturating_duration_since(opened) >= MAX_BACKOFF {
            self.failures = 0;
            self.retry_at = None;
            return;
        }

        self.failed(now);
    }

    fn delay(&self) -> Duration {
        let doublings = self.failures.saturating_sub(1).min(16);

        MIN_BACKOFF.saturating_mul(1 << doublings).min(MAX_BACKOFF)
    }
}

/// The highest positions every leader announced.
#[derive(Debug, Default)]
struct Seen {
    store: position::Position,
    categories: BTreeMap<category::CategoryId, position::Position>,
    lsn: Option<lsn::Lsn>,
    fence: Option<u64>,
}

impl Seen {
    /// Records the message's positions; `None`, recording nothing, if it holds a position above
    /// [`position::Position::MAX`].
    fn record(&mut self, message: &v1::WatchResponse) -> Option<Term> {
        let high = message.high_water()?;
        self.store = self.store.max(high.store());
        self.lsn = self.lsn.max(high.lsn());
        for (id, at) in high.categories() {
            let newest = self.categories.entry(*id).or_default();
            *newest = (*newest).max(*at);
        }

        Some(match self.fence {
            Some(fence) if fence >= message.fence => Term::Known,
            Some(_) => {
                self.fence = Some(message.fence);
                Term::New
            }
            None => {
                self.fence = Some(message.fence);
                Term::Known
            }
        })
    }

    fn high_water(&self) -> high_water::HighWater {
        high_water::HighWater::new(
            self.store,
            self.categories.iter().map(|(id, at)| (*id, *at)).collect(),
            self.lsn,
        )
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn message(
        fence: u64,
        store: u64,
        categories: &[(i32, u64)],
        lsn: Option<u64>,
    ) -> v1::WatchResponse {
        v1::WatchResponse {
            fence,
            store,
            categories: categories
                .iter()
                .map(|(id, at)| v1::CategoryPosition {
                    category_id: *id,
                    position: *at,
                })
                .collect(),
            lsn,
        }
    }

    fn at(n: u64) -> position::Position {
        position::Position::new(n).unwrap()
    }

    #[test]
    fn keeps_the_highest_positions_across_leaders() {
        let mut seen = Seen::default();

        assert_eq!(
            seen.record(&message(3, 10, &[(1, 10), (2, 4)], Some(100))),
            Some(Term::Known)
        );
        // A stale leader still streaming behind the new one.
        assert_eq!(
            seen.record(&message(2, 8, &[(1, 8), (3, 7)], Some(90))),
            Some(Term::Known)
        );
        assert_eq!(
            seen.record(&message(3, 11, &[(2, 11)], None)),
            Some(Term::Known)
        );
        assert_eq!(
            seen.record(&message(4, 11, &[], Some(120))),
            Some(Term::New)
        );
        // A position no sequencer sends is not recorded, and neither is its fence.
        assert_eq!(seen.record(&message(9, u64::MAX, &[], None)), None);
        assert_eq!(seen.record(&message(5, 11, &[], None)), Some(Term::New));

        let high = seen.high_water();
        assert_eq!(high.store(), at(11));
        assert_eq!(high.category(category::CategoryId::new(1)), at(10));
        assert_eq!(high.category(category::CategoryId::new(2)), at(11));
        assert_eq!(high.category(category::CategoryId::new(3)), at(7));
        assert_eq!(high.lsn(), Some(lsn::Lsn::new(120)));
    }

    #[test]
    fn endpoints_must_be_http_uris() {
        let store = store::StoreId::new(1);

        let parse = |text: &str| text.parse::<Endpoint>();

        let waker = GrpcWaker::new(
            store,
            pges_core::non_empty![
                parse("http://localhost:50051").unwrap(),
                parse("http://10.0.0.2:50051").unwrap()
            ],
        );
        assert_eq!(waker.endpoints.len(), 2);
        assert_eq!(
            parse("https://localhost:50051").is_ok(),
            cfg!(feature = "wake-grpc-tls")
        );
        assert!(matches!(
            parse("ftp://localhost:50051"),
            Err(EndpointError::UnsupportedScheme { .. })
        ));
        assert!(matches!(
            parse("localhost:50051"),
            Err(EndpointError::UnsupportedScheme { .. } | EndpointError::Invalid { .. })
        ));
        assert!(matches!(
            parse("http://exa mple:1"),
            Err(EndpointError::Invalid { .. })
        ));
    }

    /// A PEM section that never ends.
    #[cfg(feature = "wake-grpc-tls")]
    const UNTERMINATED: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n";
    /// A certificate whose DER is three zero bytes.
    #[cfg(feature = "wake-grpc-tls")]
    const GARBAGE_CERTIFICATE: &str =
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
    /// A PKCS#8 private key whose DER is three zero bytes.
    #[cfg(feature = "wake-grpc-tls")]
    const GARBAGE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";

    #[cfg(feature = "wake-grpc-tls")]
    #[test]
    fn valid_tls_files_are_accepted() {
        let pki = pges_testing::tls::Pki::generate().unwrap();

        let tls = ClientTls::ca(&pki.ca).unwrap();
        assert!(tls.identity(&pki.client_cert, &pki.client_key).is_ok());
    }

    #[cfg(feature = "wake-grpc-tls")]
    #[test]
    fn each_unusable_ca_is_refused_up_front_with_its_cause() {
        let pki = pges_testing::tls::Pki::generate().unwrap();
        let refused = |ca: &str| ClientTls::ca(ca).unwrap_err();

        assert_eq!(refused("not a certificate"), TlsError::NoCaCertificate);
        assert_eq!(refused(&pki.client_key), TlsError::NoCaCertificate);
        assert!(matches!(
            refused(UNTERMINATED),
            TlsError::UnreadableCaCertificate { .. }
        ));
        assert!(matches!(
            refused(&format!("{}{GARBAGE_CERTIFICATE}", pki.ca)),
            TlsError::InvalidCaCertificate { .. }
        ));
    }

    #[cfg(feature = "wake-grpc-tls")]
    #[test]
    fn each_unusable_client_identity_is_refused_up_front_with_its_cause() {
        let pki = pges_testing::tls::Pki::generate().unwrap();
        let tls = ClientTls::ca(&pki.ca).unwrap();
        let refused = |cert: &str, key: &str| tls.clone().identity(cert, key).unwrap_err();

        assert_eq!(
            refused("not a certificate", &pki.client_key),
            TlsError::NoCertificate
        );
        assert!(matches!(
            refused(UNTERMINATED, &pki.client_key),
            TlsError::UnreadableCertificate { .. }
        ));
        assert!(matches!(
            refused(GARBAGE_CERTIFICATE, &pki.client_key),
            TlsError::InvalidCertificate { .. }
        ));
        assert_eq!(
            refused(&pki.client_cert, &pki.client_cert),
            TlsError::NoPrivateKey
        );
        assert!(matches!(
            refused(&pki.client_cert, "-----BEGIN PRIVATE KEY-----\nAAAA\n"),
            TlsError::UnreadablePrivateKey { .. }
        ));
        assert_eq!(
            refused(
                &pki.client_cert,
                &format!("{}{}", pki.client_key, pki.server_key)
            ),
            TlsError::MultiplePrivateKeys { count: 2 }
        );
        assert_eq!(
            refused(&pki.client_cert, &pki.server_key),
            TlsError::KeyMismatch
        );
        assert!(matches!(
            refused(&pki.client_cert, GARBAGE_KEY),
            TlsError::Rejected { .. }
        ));
    }

    #[test]
    fn the_backoff_doubles_up_to_its_cap_and_resets_after_a_lasting_stream() {
        let now = Instant::now();
        let mut backoff = Backoff::default();

        let mut delays = Vec::new();
        for _ in 0..8 {
            backoff.failed(now);
            delays.push(backoff.retry_at.unwrap() - now);
        }
        let ms = Duration::from_millis;
        assert_eq!(
            delays,
            [
                ms(100),
                ms(200),
                ms(400),
                ms(800),
                ms(1600),
                ms(3200),
                ms(5000),
                ms(5000)
            ]
        );

        backoff.connected();
        backoff.lost(now, now + ms(10));
        assert_eq!(backoff.retry_at, Some(now + ms(10) + MAX_BACKOFF));
        backoff.connected();
        backoff.lost(now, now + MAX_BACKOFF);
        assert_eq!(backoff.retry_at, None);
        backoff.failed(now);
        assert_eq!(backoff.retry_at, Some(now + MIN_BACKOFF));
    }

    /// Endpoints that accept connections and never answer, so each attempt lasts the whole connect timeout.
    async fn silent_endpoints(
        count: usize,
    ) -> (non_empty::NonEmpty<Endpoint>, tokio::task::JoinSet<()>) {
        let mut endpoints = Vec::new();
        let mut servers = tokio::task::JoinSet::new();
        for _ in 0..count {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            endpoints.push(format!("http://{address}").parse::<Endpoint>().unwrap());
            servers.spawn(async move {
                let mut held = Vec::new();
                while let Ok((socket, _)) = listener.accept().await {
                    held.push(socket);
                }
            });
        }
        (non_empty::NonEmpty::try_from(endpoints).unwrap(), servers)
    }

    /// A subscriber that drops every call before an attempt can time out (to renew its lease, say) still
    /// learns that no endpoint serves.
    #[tokio::test]
    async fn slow_endpoints_are_reported_even_when_every_call_is_dropped() {
        let (endpoints, _servers) = silent_endpoints(2).await;
        let config = GrpcConfig {
            connect_timeout: pges_core::interval!(400 ms),
            unreachable_after: Duration::from_millis(300),
            ..GrpcConfig::default()
        };
        let mut waker = GrpcWaker::with_config(store::StoreId::new(1), endpoints, config);
        let known = high_water::HighWater::default();

        for _ in 0..10 {
            let call = waker::Waker::changed(&mut waker, &known);
            match tokio::time::timeout(Duration::from_millis(200), call).await {
                Err(_) => continue,
                Ok(Ok(high)) => panic!("woken without a stream: {high:?}"),
                Ok(Err(waker::WakeError::Other(error))) => {
                    let unreachable = error.downcast_ref::<Unreachable>().unwrap();
                    assert!(unreachable.unreachable() >= Duration::from_millis(300));
                    assert!(
                        unreachable.to_string().contains("interrupted"),
                        "{unreachable}"
                    );
                    return;
                }
                Ok(Err(error)) => panic!("not an Unreachable error: {error:?}"),
            }
        }
        panic!("the waker never reported the endpoints unreachable");
    }

    /// What a fake watch server does with every `Watch` call.
    #[derive(Clone)]
    enum Behaviour {
        Refuse(tonic::Code),
        Send(Vec<v1::WatchResponse>),
    }

    struct FakeWatch(Behaviour);

    #[tonic::async_trait]
    impl v1::watch_server::Watch for FakeWatch {
        type WatchStream =
            futures_util::stream::BoxStream<'static, Result<v1::WatchResponse, tonic::Status>>;

        async fn watch(
            &self,
            _request: tonic::Request<v1::WatchRequest>,
        ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
            use futures_util::StreamExt as _;

            match &self.0 {
                Behaviour::Refuse(code) => Err(tonic::Status::new(*code, "the fake refuses")),
                Behaviour::Send(messages) => Ok(tonic::Response::new(
                    futures_util::stream::iter(messages.clone().into_iter().map(Ok))
                        .chain(futures_util::stream::pending())
                        .boxed(),
                )),
            }
        }

        async fn status(
            &self,
            _request: tonic::Request<v1::StatusRequest>,
        ) -> Result<tonic::Response<v1::StatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("the fake has no status"))
        }
    }

    /// A fake watch server on a local port, until the returned task is dropped.
    async fn fake(behaviour: Behaviour) -> (Endpoint, tokio::task::JoinSet<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let mut server = tokio::task::JoinSet::new();
        server.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(v1::watch_server::WatchServer::new(FakeWatch(behaviour)))
                .serve_with_incoming(incoming)
                .await;
        });
        (endpoint, server)
    }

    /// The call's result within `wait`, or `None` if it was still waiting.
    async fn within(
        waker: &mut GrpcWaker,
        wait: Duration,
    ) -> Option<Result<high_water::HighWater, waker::WakeError>> {
        let known = high_water::HighWater::default();
        tokio::time::timeout(wait, waker::Waker::changed(waker, &known))
            .await
            .ok()
    }

    fn rejection(result: Option<Result<high_water::HighWater, waker::WakeError>>) -> Rejection {
        match result {
            Some(Err(waker::WakeError::Other(error))) => {
                error.downcast_ref::<Rejected>().unwrap().rejection()
            }
            other => panic!("not a Rejected error: {other:?}"),
        }
    }

    /// A server that refuses in a way retrying cannot fix fails the call at once, long before
    /// `unreachable_after`, and again on the next call after the backoff.
    #[tokio::test]
    async fn refusals_retrying_cannot_fix_are_reported_at_once() {
        for (code, expected) in [
            (tonic::Code::Unimplemented, Rejection::Unimplemented),
            (tonic::Code::InvalidArgument, Rejection::InvalidArgument),
            (tonic::Code::PermissionDenied, Rejection::PermissionDenied),
            (tonic::Code::Unauthenticated, Rejection::Unauthenticated),
            (
                tonic::Code::FailedPrecondition,
                Rejection::FailedPrecondition,
            ),
        ] {
            let (endpoint, _server) = fake(Behaviour::Refuse(code)).await;
            let mut waker = GrpcWaker::new(
                store::StoreId::new(1),
                non_empty::NonEmpty::new(endpoint, []),
            );

            let first = within(&mut waker, Duration::from_secs(5)).await;
            assert_eq!(rejection(first), expected, "{code:?}");
            let again = within(&mut waker, Duration::from_secs(5)).await;
            assert_eq!(rejection(again), expected, "{code:?}");
        }
    }

    /// Failures that may pass are retried silently until `unreachable_after`.
    #[tokio::test]
    async fn transient_refusals_are_retried() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::ResourceExhausted,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Internal,
        ] {
            let (endpoint, _server) = fake(Behaviour::Refuse(code)).await;
            let config = GrpcConfig {
                unreachable_after: Duration::from_millis(600),
                ..GrpcConfig::default()
            };
            let mut waker = GrpcWaker::with_config(
                store::StoreId::new(1),
                non_empty::NonEmpty::new(endpoint, []),
                config,
            );

            assert!(
                within(&mut waker, Duration::from_millis(300))
                    .await
                    .is_none(),
                "{code:?}"
            );
            let reported = within(&mut waker, Duration::from_secs(5)).await;
            let Some(Err(waker::WakeError::Other(error))) = reported else {
                panic!("{code:?}: not reported: {reported:?}");
            };
            assert!(
                error.downcast_ref::<Unreachable>().is_some(),
                "{code:?}: {error}"
            );
        }
    }

    /// An endpoint that refuses is only logged while another one serves.
    #[tokio::test]
    async fn a_refusal_another_endpoint_makes_up_for_is_not_reported() {
        let (refusing, _refusing) = fake(Behaviour::Refuse(tonic::Code::Unimplemented)).await;
        let (serving, _serving) = fake(Behaviour::Send(vec![message(1, 5, &[], None)])).await;
        let mut waker = GrpcWaker::new(
            store::StoreId::new(1),
            non_empty::NonEmpty::new(refusing, [serving]),
        );

        let woken = within(&mut waker, Duration::from_secs(5)).await;
        assert_eq!(woken.unwrap().unwrap().store(), at(5));
    }

    /// A position no sequencer sends fails the call that received it, and every later stream that sends one
    /// again: no silent reconnect loop.
    #[tokio::test]
    async fn a_position_no_sequencer_sends_is_reported() {
        let garbage = message(1, u64::MAX, &[], None);
        let (endpoint, _server) = fake(Behaviour::Send(vec![garbage])).await;
        let mut waker = GrpcWaker::new(
            store::StoreId::new(1),
            non_empty::NonEmpty::new(endpoint, []),
        );

        for _ in 0..3 {
            let reported = within(&mut waker, Duration::from_secs(10)).await;
            assert_eq!(rejection(reported), Rejection::Protocol);
        }
    }

    fn messages() -> impl Strategy<Value = Vec<v1::WatchResponse>> {
        let message = (
            0_u64..5,
            0_u64..1000,
            prop::collection::vec((1_i32..6, 0_u64..1000), 0..6),
            prop::option::of(0_u64..1000),
        )
            .prop_map(|(fence, store, categories, lsn)| message(fence, store, &categories, lsn));

        prop::collection::vec(message, 1..40)
    }

    proptest! {
        /// Whatever order values arrive in, from whichever leaders, the reported high-water mark never goes
        /// down, and it covers every value received.
        #[test]
        fn never_goes_backwards(received in messages()) {
            let mut seen = Seen::default();
            let mut before = seen.high_water();

            for message in &received {
                seen.record(message);
                let now = seen.high_water();
                let value = message.high_water().unwrap();

                prop_assert!(now.store() >= before.store());
                prop_assert!(now.lsn() >= before.lsn());
                for (id, at) in before.categories() {
                    prop_assert!(now.category(*id) >= *at);
                }
                prop_assert!(now.store() >= value.store());
                prop_assert!(now.lsn() >= value.lsn());
                for (id, at) in value.categories() {
                    prop_assert!(now.category(*id) >= *at);
                }
                before = now;
            }
        }
    }
}
