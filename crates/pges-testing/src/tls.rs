//! Throwaway certificates for TLS tests (feature `tls`), generated per test, so no key is checked in.

use anyhow::Result;

/// A CA, a server certificate it signed for `localhost` and `127.0.0.1`, one it signed for another host, a
/// client certificate it signed, and an unrelated CA, all PEM.
#[derive(Debug, Clone)]
pub struct Pki {
    /// The CA that signed the server and client certificates.
    pub ca: String,
    /// A CA that signed nothing here: trusting it alone must refuse the server.
    pub other_ca: String,
    /// The server's certificate, for `localhost` and `127.0.0.1`.
    pub server_cert: String,
    /// The server certificate's private key.
    pub server_key: String,
    /// A server certificate the CA signed for `elsewhere.invalid` only: a client must refuse it on
    /// `localhost`.
    pub elsewhere_cert: String,
    /// The private key of `elsewhere_cert`.
    pub elsewhere_key: String,
    /// A client certificate, for mutual TLS.
    pub client_cert: String,
    /// The client certificate's private key.
    pub client_key: String,
}

impl Pki {
    /// Generates a new set.
    pub fn generate() -> Result<Self> {
        let ca = ca("pges test CA")?;
        let other_ca = ca_pem("pges unrelated CA")?;

        let (server_cert, server_key) = server(&ca, &["localhost", "127.0.0.1"])?;
        let (elsewhere_cert, elsewhere_key) = server(&ca, &["elsewhere.invalid"])?;

        let client_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
        let mut client = rcgen::CertificateParams::new(Vec::<String>::new())?;
        client
            .distinguished_name
            .push(rcgen::DnType::CommonName, "pges test client");
        client.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = client.signed_by(&client_key, &ca)?;

        Ok(Self {
            ca: ca.pem(),
            other_ca,
            server_cert,
            server_key,
            elsewhere_cert,
            elsewhere_key,
            client_cert: client_cert.pem(),
            client_key: client_key.serialize_pem(),
        })
    }
}

fn ca(name: &str) -> Result<rcgen::CertifiedIssuer<'static, rcgen::KeyPair>> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];

    Ok(rcgen::CertifiedIssuer::self_signed(params, key)?)
}

/// A server certificate `ca` signed for `names`, and its key.
fn server(
    ca: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    names: &[&str],
) -> Result<(String, String)> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
    let mut params = rcgen::CertificateParams::new(
        names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
    )?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "pges test server");
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.signed_by(&key, ca)?;

    Ok((cert.pem(), key.serialize_pem()))
}

fn ca_pem(name: &str) -> Result<String> {
    Ok(ca(name)?.pem())
}
