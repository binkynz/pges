//! [`TlsError`], why TLS settings could not be built from the PEM certificates and key given.
//!
//! The gRPC watch server's `ServerTls` (`pges-sequencer`, feature `watch-grpc-tls`) and the gRPC waker's
//! `ClientTls` (`pges-subscribe`, feature `wake-grpc-tls`) return it, and re-export it as `grpc::TlsError`.
//! This crate has no TLS code: the type lives here so that both return the same one.

/// TLS settings could not be built from the PEM certificates and key given. Each is checked when the
/// settings are built, not when a connection is made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TlsError {
    /// The certificate chain's PEM holds no certificate.
    #[error("the certificate PEM holds no certificate")]
    NoCertificate,
    /// The certificate chain's PEM cannot be parsed: a section has no end, or its base64 is invalid.
    #[error("the certificate PEM is unreadable: {reason}")]
    UnreadableCertificate {
        /// The PEM parser's message.
        reason: String,
    },
    /// The chain's first certificate is not a valid X.509 certificate.
    #[error("the certificate is not a valid X.509 certificate: {reason}")]
    InvalidCertificate {
        /// The TLS library's message.
        reason: String,
    },
    /// The key's PEM holds no private key (PKCS#1, PKCS#8 or SEC1).
    #[error("the key PEM holds no private key")]
    NoPrivateKey,
    /// The key's PEM cannot be parsed: a section has no end, or its base64 is invalid.
    #[error("the key PEM is unreadable: {reason}")]
    UnreadablePrivateKey {
        /// The PEM parser's message.
        reason: String,
    },
    /// The key's PEM holds more than one private key, so which to use is ambiguous.
    #[error("the key PEM holds {count} private keys; exactly one is required")]
    MultiplePrivateKeys {
        /// How many private keys it holds.
        count: usize,
    },
    /// The private key is not the one the chain's first certificate was issued for.
    #[error("the private key does not match the certificate")]
    KeyMismatch,
    /// The CA's PEM holds no certificate.
    #[error("the CA PEM holds no certificate")]
    NoCaCertificate,
    /// The CA's PEM cannot be parsed: a section has no end, or its base64 is invalid.
    #[error("the CA PEM is unreadable: {reason}")]
    UnreadableCaCertificate {
        /// The PEM parser's message.
        reason: String,
    },
    /// A certificate in the CA's PEM cannot be a trust anchor, for example because it is not valid X.509.
    #[error("the CA PEM holds a certificate that cannot be a trust anchor: {reason}")]
    InvalidCaCertificate {
        /// The TLS library's message.
        reason: String,
    },
    /// The TLS library refused the settings for another reason, for example a private key of an algorithm
    /// it does not support, or (with the system's roots) no root certificate found.
    #[error("the TLS settings were refused: {reason}")]
    Rejected {
        /// The TLS library's message, with its causes.
        reason: String,
    },
}
