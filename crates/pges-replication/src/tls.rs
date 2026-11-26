//! TLS over TCP: negotiation, certificate verification, and the `tls-server-end-point` channel binding data.

use std::{
    net::Ipv4Addr,
    path::Path,
    sync::{Arc, OnceLock},
};

use bytes::BytesMut;
use postgres_protocol::message::frontend;
use rustls::{
    DigitallySignedStruct, RootCertStore, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::CryptoProvider,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject};
use sha2::Digest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::{config, error};

/// The outcome of asking for TLS on a fresh TCP connection.
pub(crate) enum Negotiated {
    Encrypted(Box<TlsStream<TcpStream>>),
    /// The server answered the `SSLRequest` with `N`; the socket is still usable without TLS.
    Declined(TcpStream),
}

/// Starts TLS on `tcp`, by `SSLRequest` or directly, as `config` says.
pub(crate) async fn connect(
    mut tcp: TcpStream,
    host: &str,
    config: &config::Config,
) -> Result<Negotiated, error::ConnectError> {
    let connector = TlsConnector::from(Arc::new(client_config(config)?));
    let server_name = server_name(host, config.get_ssl_mode())?;

    if config.get_ssl_negotiation() == config::SslNegotiation::Postgres {
        let mut request = BytesMut::new();
        frontend::ssl_request(&mut request);
        tcp.write_all(&request).await?;
        // Exactly one byte: anything the server sends after `S` belongs to the TLS handshake.
        match tcp.read_u8().await? {
            b'S' => {}
            b'N' => return Ok(Negotiated::Declined(tcp)),
            _ => {
                return Err(error::ConnectError::Protocol(
                    "unexpected answer to the SSLRequest".to_owned(),
                ));
            }
        }
    }

    let stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(handshake_error)?;
    Ok(Negotiated::Encrypted(Box::new(stream)))
}

/// The `tls-server-end-point` channel binding data (RFC 5929): the server certificate hashed with the hash
/// of its signature algorithm, SHA-256 when that is MD5 or SHA-1.
///
/// `None` when the signature algorithm has no single hash (e.g. Ed25519) or the certificate does not parse;
/// PostgreSQL cannot bind to such a certificate either.
pub(crate) fn server_end_point(certificate: &[u8]) -> Option<Vec<u8>> {
    let digest = match signature_hash(certificate)? {
        Hash::Sha224 => sha2::Sha224::digest(certificate).to_vec(),
        Hash::Sha256 => sha2::Sha256::digest(certificate).to_vec(),
        Hash::Sha384 => sha2::Sha384::digest(certificate).to_vec(),
        Hash::Sha512 => sha2::Sha512::digest(certificate).to_vec(),
    };
    Some(digest)
}

fn client_config(config: &config::Config) -> Result<rustls::ClientConfig, error::TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier: Arc<dyn ServerCertVerifier> = match (config.get_ssl_mode(), roots(config)?) {
        (config::SslMode::VerifyFull, Some(roots)) => {
            rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider.clone())
                .build()
                .map_err(|e| error::TlsError::RootCertificates(e.to_string()))?
        }
        (_, Some(roots)) => Arc::new(ChainOnly {
            roots,
            provider: provider.clone(),
        }),
        (_, None) => Arc::new(Unverified {
            provider: provider.clone(),
        }),
    };

    let mut client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| error::TlsError::Handshake(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    // PostgreSQL 17+ requires it for direct TLS and checks it, when sent, after an SSLRequest.
    client_config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(client_config)
}

/// The roots that verify the server, or `None` when the certificate is not checked.
fn roots(config: &config::Config) -> Result<Option<Arc<RootCertStore>>, error::TlsError> {
    let verifies = matches!(
        config.get_ssl_mode(),
        config::SslMode::VerifyCa | config::SslMode::VerifyFull
    );
    match config.get_root_certificates() {
        Some(config::RootCertificates::File(path)) => load_file(path).map(Some),
        Some(config::RootCertificates::System) => system_roots().map(Some),
        None if verifies => system_roots().map(Some),
        None => Ok(None),
    }
}

fn load_file(path: &Path) -> Result<Arc<RootCertStore>, error::TlsError> {
    let failed =
        |reason: String| error::TlsError::RootCertificates(format!("{}: {reason}", path.display()));
    let pem = std::fs::read(path).map_err(|e| failed(e.to_string()))?;

    let mut store = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        let certificate = certificate.map_err(|e| failed(e.to_string()))?;
        store.add(certificate).map_err(|e| failed(e.to_string()))?;
    }
    if store.is_empty() {
        return Err(failed("no certificate in the file".to_owned()));
    }
    Ok(Arc::new(store))
}

fn system_roots() -> Result<Arc<RootCertStore>, error::TlsError> {
    static SYSTEM_ROOTS: OnceLock<Arc<RootCertStore>> = OnceLock::new();
    if let Some(roots) = SYSTEM_ROOTS.get() {
        return Ok(roots.clone());
    }

    let loaded = rustls_native_certs::load_native_certs();
    let mut store = RootCertStore::empty();
    store.add_parsable_certificates(loaded.certs);
    if store.is_empty() {
        let errors = loaded
            .errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        return Err(error::TlsError::RootCertificates(format!(
            "the platform trust store has no usable certificate ({errors})"
        )));
    }
    Ok(SYSTEM_ROOTS.get_or_init(|| Arc::new(store)).clone())
}

fn server_name(host: &str, mode: config::SslMode) -> Result<ServerName<'static>, error::TlsError> {
    match ServerName::try_from(host.to_owned()) {
        Ok(name) => Ok(name),
        Err(_) if mode == config::SslMode::VerifyFull => {
            Err(error::TlsError::InvalidServerName(host.to_owned()))
        }
        // The name is only used for SNI here, and an IP address sends none.
        Err(_) => Ok(ServerName::IpAddress(Ipv4Addr::UNSPECIFIED.into())),
    }
}

fn handshake_error(e: std::io::Error) -> error::ConnectError {
    let Some(tls) = e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    else {
        return error::ConnectError::Io(e);
    };

    let certificate = match tls {
        rustls::Error::InvalidCertificate(certificate) => certificate,
        other => return error::TlsError::Handshake(other.to_string()).into(),
    };
    let reason = match certificate {
        rustls::CertificateError::UnknownIssuer => error::CertificateError::UnknownIssuer,
        rustls::CertificateError::NotValidForName
        | rustls::CertificateError::NotValidForNameContext { .. } => {
            error::CertificateError::WrongHostName
        }
        rustls::CertificateError::Expired | rustls::CertificateError::ExpiredContext { .. } => {
            error::CertificateError::Expired
        }
        rustls::CertificateError::NotValidYet
        | rustls::CertificateError::NotValidYetContext { .. } => {
            error::CertificateError::NotYetValid
        }
        other => error::CertificateError::Other(other.to_string()),
    };
    error::TlsError::Certificate(reason).into()
}

/// Accepts any certificate, but still checks that the server holds its key. Used when the mode does not
/// verify and no roots are configured, as libpq does.
#[derive(Debug)]
struct Unverified {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Checks the chain against the roots but not the host name (`verify-ca`).
#[derive(Debug)]
struct ChainOnly {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let certificate = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Hash {
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

/// The hash of the certificate's `signatureAlgorithm`, as PostgreSQL picks it for `tls-server-end-point`.
fn signature_hash(certificate: &[u8]) -> Option<Hash> {
    let (certificate, _) = der_element(certificate, DER_SEQUENCE)?;
    let (_tbs, rest) = der_element(certificate, DER_SEQUENCE)?;
    let (algorithm, _) = der_element(rest, DER_SEQUENCE)?;
    let (oid, parameters) = der_element(algorithm, DER_OID)?;

    if oid == OID_RSASSA_PSS {
        return pss_hash(parameters);
    }
    SIGNATURE_HASHES
        .iter()
        .find(|(known, _)| *known == oid)
        .map(|(_, hash)| *hash)
}

/// RSASSA-PSS names its hash in the parameters: `[0] EXPLICIT AlgorithmIdentifier DEFAULT sha1`.
fn pss_hash(parameters: &[u8]) -> Option<Hash> {
    let (parameters, _) = der_element(parameters, DER_SEQUENCE)?;
    let Some((hash_algorithm, _)) = der_element(parameters, DER_CONTEXT_0) else {
        return Some(Hash::Sha256);
    };
    let (hash_algorithm, _) = der_element(hash_algorithm, DER_SEQUENCE)?;
    let (oid, _) = der_element(hash_algorithm, DER_OID)?;
    DIGEST_HASHES
        .iter()
        .find(|(known, _)| *known == oid)
        .map(|(_, hash)| *hash)
}

/// Splits the DER element with `tag` at the start of `input` into its contents and what follows it.
fn der_element(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&found, rest) = input.split_first()?;
    if found != tag {
        return None;
    }

    let (&first, rest) = rest.split_first()?;
    let (length, rest) = if first < 0x80 {
        (usize::from(first), rest)
    } else {
        let width = usize::from(first & 0x7f);
        if width == 0 || width > 4 || rest.len() < width {
            return None;
        }
        let (bytes, rest) = rest.split_at(width);
        let length = bytes
            .iter()
            .fold(0usize, |length, byte| (length << 8) | usize::from(*byte));
        (length, rest)
    };
    if rest.len() < length {
        return None;
    }
    Some(rest.split_at(length))
}

const DER_SEQUENCE: u8 = 0x30;
const DER_OID: u8 = 0x06;
const DER_CONTEXT_0: u8 = 0xa0;

/// 1.2.840.113549.1.1.10
const OID_RSASSA_PSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0a];

/// Signature algorithms and the hash `tls-server-end-point` uses for them. MD5 and SHA-1 map to SHA-256
/// (RFC 5929, section 4.1).
const SIGNATURE_HASHES: &[(&[u8], Hash)] = &[
    // md5WithRSAEncryption, sha1WithRSAEncryption, sha256/384/512/224WithRSAEncryption
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x04],
        Hash::Sha256,
    ),
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05],
        Hash::Sha256,
    ),
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b],
        Hash::Sha256,
    ),
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0c],
        Hash::Sha384,
    ),
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d],
        Hash::Sha512,
    ),
    (
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0e],
        Hash::Sha224,
    ),
    // ecdsa-with-SHA1, ecdsa-with-SHA224/256/384/512
    (&[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01], Hash::Sha256),
    (
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x01],
        Hash::Sha224,
    ),
    (
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02],
        Hash::Sha256,
    ),
    (
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03],
        Hash::Sha384,
    ),
    (
        &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04],
        Hash::Sha512,
    ),
    // dsa-with-sha1, dsa-with-sha224/256
    (&[0x2a, 0x86, 0x48, 0xce, 0x38, 0x04, 0x03], Hash::Sha256),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x03, 0x01],
        Hash::Sha224,
    ),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x03, 0x02],
        Hash::Sha256,
    ),
];

/// Digest algorithms, as RSASSA-PSS parameters name them.
const DIGEST_HASHES: &[(&[u8], Hash)] = &[
    // sha1, sha256, sha384, sha512, sha224
    (&[0x2b, 0x0e, 0x03, 0x02, 0x1a], Hash::Sha256),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01],
        Hash::Sha256,
    ),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02],
        Hash::Sha384,
    ),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03],
        Hash::Sha512,
    ),
    (
        &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x04],
        Hash::Sha224,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn self_signed(algorithm: &'static rcgen::SignatureAlgorithm) -> Vec<u8> {
        let key = rcgen::KeyPair::generate_for(algorithm).unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params.self_signed(&key).unwrap().der().to_vec()
    }

    /// A certificate shell with an empty TBS and the given algorithm identifier contents.
    fn shell(algorithm: &[u8]) -> Vec<u8> {
        let mut identifier = vec![DER_SEQUENCE, u8::try_from(algorithm.len()).unwrap()];
        identifier.extend_from_slice(algorithm);
        let mut body = vec![DER_SEQUENCE, 0];
        body.extend_from_slice(&identifier);
        body.extend_from_slice(&[0x03, 0x01, 0x00]);
        let mut certificate = vec![DER_SEQUENCE, u8::try_from(body.len()).unwrap()];
        certificate.extend_from_slice(&body);
        certificate
    }

    fn oid(bytes: &[u8]) -> Vec<u8> {
        let mut element = vec![DER_OID, u8::try_from(bytes.len()).unwrap()];
        element.extend_from_slice(bytes);
        element
    }

    #[test]
    fn hashes_ecdsa_certificates_with_their_signature_hash() {
        let p256 = self_signed(&rcgen::PKCS_ECDSA_P256_SHA256);
        assert_eq!(signature_hash(&p256), Some(Hash::Sha256));
        assert_eq!(
            server_end_point(&p256).unwrap(),
            sha2::Sha256::digest(&p256).to_vec()
        );

        let p384 = self_signed(&rcgen::PKCS_ECDSA_P384_SHA384);
        assert_eq!(signature_hash(&p384), Some(Hash::Sha384));
        assert_eq!(server_end_point(&p384).unwrap().len(), 48);
    }

    #[test]
    fn has_no_end_point_for_ed25519() {
        let ed25519 = self_signed(&rcgen::PKCS_ED25519);
        assert_eq!(server_end_point(&ed25519), None);
    }

    #[test]
    fn maps_weak_and_rsa_hashes() {
        let rsa = |last: u8| oid(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, last]);
        for (last, hash) in [
            (0x04, Hash::Sha256),
            (0x05, Hash::Sha256),
            (0x0b, Hash::Sha256),
            (0x0c, Hash::Sha384),
            (0x0d, Hash::Sha512),
            (0x0e, Hash::Sha224),
        ] {
            let mut algorithm = rsa(last);
            algorithm.extend_from_slice(&[0x05, 0x00]);
            assert_eq!(signature_hash(&shell(&algorithm)), Some(hash), "{last:#x}");
        }
        assert_eq!(signature_hash(&shell(&rsa(0x01))), None);
    }

    #[test]
    fn reads_the_pss_hash_from_its_parameters() {
        let mut default_parameters = oid(OID_RSASSA_PSS);
        default_parameters.extend_from_slice(&[DER_SEQUENCE, 0]);
        assert_eq!(
            signature_hash(&shell(&default_parameters)),
            Some(Hash::Sha256)
        );

        let sha384 = oid(&[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02]);
        let mut hash_algorithm = vec![DER_SEQUENCE, u8::try_from(sha384.len()).unwrap()];
        hash_algorithm.extend_from_slice(&sha384);
        let mut explicit = vec![DER_CONTEXT_0, u8::try_from(hash_algorithm.len()).unwrap()];
        explicit.extend_from_slice(&hash_algorithm);
        let mut algorithm = oid(OID_RSASSA_PSS);
        algorithm.extend_from_slice(&[DER_SEQUENCE, u8::try_from(explicit.len()).unwrap()]);
        algorithm.extend_from_slice(&explicit);
        assert_eq!(signature_hash(&shell(&algorithm)), Some(Hash::Sha384));
    }

    #[test]
    fn rejects_truncated_der() {
        let p256 = self_signed(&rcgen::PKCS_ECDSA_P256_SHA256);
        assert_eq!(signature_hash(&p256[..p256.len() / 2]), None);
        assert_eq!(signature_hash(&[]), None);
        assert_eq!(
            der_element(&[DER_SEQUENCE, 0x85, 1, 1, 1, 1, 1], DER_SEQUENCE),
            None
        );
    }

    #[test]
    fn verify_full_needs_a_valid_server_name() {
        assert_eq!(
            server_name("bad name", config::SslMode::VerifyFull).unwrap_err(),
            error::TlsError::InvalidServerName("bad name".into())
        );
        assert!(server_name("bad name", config::SslMode::Require).is_ok());
        assert!(server_name("db.example", config::SslMode::VerifyFull).is_ok());
        assert!(server_name("::1", config::SslMode::VerifyFull).is_ok());
    }
}
