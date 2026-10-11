//! Ephemeral TLS trust for local tests and benchmarks; no stored private keys.
/// Deterministic inputs shared by offline correctness checks and workloads.
pub mod fixture;
pub mod records;
/// Shared passive frame association for authenticated transport experiments.
pub mod frames {
    pub use crate::frame_tracker::{Metadata, Tracker};
}
use std::sync::Arc;

/// A server identity and client trust store generated for one local experiment.
pub struct TlsPeer {
    /// Client verifies the generated certificate and loopback server name.
    pub client: Arc<rustls::ClientConfig>,
    /// Server presents the generated identity.
    pub server: Arc<rustls::ServerConfig>,
    /// Public certificate DER for explicitly trusted network fixtures. The
    /// generated signing key remains only inside the server configuration.
    pub certificate: rustls::pki_types::CertificateDer<'static>,
}
impl TlsPeer {
    /// Generate fresh localhost test identity, never public production trust.
    ///
    /// # Errors
    /// Returns certificate generation or TLS configuration errors.
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::with_builders(
            rustls::ClientConfig::builder(),
            rustls::ServerConfig::builder(),
        )
    }
    /// Construct fixture-local trust with an explicit provider, without changing
    /// the process default. Both peers retain safe protocol versions and normal
    /// certificate/hostname verification.
    ///
    /// # Errors
    /// Rejects incompatible provider/version combinations or certificate errors.
    pub fn with_provider(
        provider: Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // https://docs.rs/rustls/0.23.45/rustls/crypto/struct.CryptoProvider.html
        Self::with_builders(
            rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()?,
            rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?,
        )
    }
    fn with_builders(
        client: rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>,
        server: rustls::ConfigBuilder<rustls::ServerConfig, rustls::WantsVerifier>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // https://docs.rs/rcgen/0.14.10/rcgen/fn.generate_simple_self_signed.html
        let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![
            "localhost".into(),
            "127.0.0.1".into(),
            "::1".into(),
        ])?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone())?;
        let client = client.with_root_certificates(roots).with_no_client_auth();
        let server = server.with_no_client_auth().with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
        )?;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            certificate: cert.der().clone(),
        })
    }
}
