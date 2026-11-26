//! The gRPC watch server's and the gRPC waker's TLS settings fail with one error type, reachable through
//! the facade under both paths and from `pges::core`.

#![cfg(all(feature = "watch-grpc-tls", feature = "wake-grpc-tls"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pges::core::tls;
use pges::sequencer::grpc as watch_grpc;
use pges::subscribe::waker::grpc as wake_grpc;

#[test]
fn both_grpc_modules_return_the_core_tls_error() {
    let server: watch_grpc::TlsError = watch_grpc::ServerTls::new("", "").unwrap_err();
    let client: wake_grpc::TlsError = wake_grpc::ClientTls::ca("").unwrap_err();

    let errors: [tls::TlsError; 2] = [server, client];
    assert_eq!(
        errors,
        [tls::TlsError::NoCertificate, tls::TlsError::NoCaCertificate]
    );
}
