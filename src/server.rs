use axum_server::tls_rustls::RustlsConfig;
use axum_server::HttpConfig;
use hyper::server::accept::Accept;
use itertools::Itertools;
use rustls::server::AllowAnyAuthenticatedClient;
use rustls::Certificate;
use rustls::PrivateKey;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls_pemfile::certs;
use rustls_pemfile::read_one;
use rustls_pemfile::Item;
use std::iter;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::remove_file;
use tokio::fs::set_permissions;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

pub struct TlsCfg {
  // These should be in the PEM format.
  cert: Vec<u8>,
  key: Vec<u8>,
  // If set, mutual TLS will be enabled, and clients must send a valid certificate.
  ca: Option<Vec<u8>>,
}

pub async fn build_unix_socket_server(
  path: &Path,
  mode: u32,
) -> hyper::server::Builder<impl Accept<Conn = tokio::net::UnixStream, Error = std::io::Error>> {
  let _ = remove_file(path).await;
  let unix_listener = UnixListener::bind(path).expect("failed to bind UNIX socket");
  let stream = UnixListenerStream::new(unix_listener);
  let acceptor = hyper::server::accept::from_stream(stream);
  set_permissions(path, PermissionsExt::from_mode(mode))
    .await
    .unwrap();
  axum::Server::builder(acceptor)
}

pub fn build_port_server(
  interface: Ipv4Addr,
  port: u16,
) -> hyper::server::Builder<hyper::server::conn::AddrIncoming> {
  let addr = SocketAddr::from((interface, port));
  axum::Server::bind(&addr)
}

pub fn build_port_server_with_tls(
  interface: Ipv4Addr,
  port: u16,
  tls: &TlsCfg,
) -> axum_server::Server<axum_server::tls_rustls::RustlsAcceptor> {
  fn priv_keys(p: &[u8]) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    // WARNING: This must not be inlined, as that would reset the cursor endlessly.
    let mut buf = p;
    for item in iter::from_fn(|| read_one(&mut buf).transpose()) {
      match item.expect(&format!("read item from {p:?}")) {
        Item::ECKey(k) | Item::PKCS8Key(k) | Item::RSAKey(k) => keys.push(k),
        _ => {}
      };
    }
    keys
  }

  let tls_config = ServerConfig::builder().with_safe_defaults();

  let tls_config = match &tls.ca {
    Some(ca) => {
      let mut roots = RootCertStore::empty();
      for cert in certs(&mut ca.as_slice()).expect("read SSL CA PEM file certificates") {
        roots
          .add(&Certificate(cert))
          .expect("add SSL CA certificate");
      }
      tls_config.with_client_cert_verifier(AllowAnyAuthenticatedClient::new(roots).boxed())
    }
    None => tls_config.with_no_client_auth(),
  };

  let mut tls_config = tls_config
    .with_single_cert(
      certs(&mut tls.cert.as_slice())
        .expect("read certificates in SSL certificate PEM file")
        .into_iter()
        .map(|c| Certificate(c))
        .collect_vec(),
      PrivateKey(
        priv_keys(&tls.key)
          .pop()
          .expect("no private key in SSL private key PEM file"),
      ),
    )
    .expect("build SSL");

  // https://github.com/programatik29/axum-server/blob/86bc6e7311959285ff00815843a8d702affe51d9/src/tls_rustls/mod.rs#L278C7-L278C7
  tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

  let addr = SocketAddr::from((interface, port));
  axum_server::bind_rustls(addr, RustlsConfig::from_config(Arc::new(tls_config))).http_config(
    // These larger values make a significant performance improvement over long fat pipes.
    HttpConfig::new()
      .http2_initial_connection_window_size(1024 * 1024 * 1024)
      .http2_initial_stream_window_size(1024 * 1024 * 1024)
      .http2_max_frame_size(16777215) // This is the maximum supported: https://github.com/hyperium/h2/blob/633116ef68b4e7b5c4c5699fb5d10b58ef5818ac/src/frame/settings.rs#L53C11-L53C29.
      .http2_max_pending_accept_reset_streams(2048)
      .http2_max_send_buf_size(1024 * 1024 * 64)
      .build(),
  )
}
