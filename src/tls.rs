use std::future::Future;
use std::sync::Once;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_native_tls::{TlsConnector, TlsStream};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect};

static RUSTLS_PROVIDER: Once = Once::new();

pub fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Clone)]
pub struct MakeNativeTlsConnect {
    connector: TlsConnector,
}

impl MakeNativeTlsConnect {
    pub fn new(connector: native_tls::TlsConnector) -> Self {
        Self {
            connector: TlsConnector::from(connector),
        }
    }
}

impl<S> MakeTlsConnect<S> for MakeNativeTlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = NativeTlsStream<S>;
    type TlsConnect = NativeTlsConnect;
    type Error = std::convert::Infallible;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        Ok(NativeTlsConnect {
            hostname: hostname.to_string(),
            connector: self.connector.clone(),
        })
    }
}

pub struct NativeTlsConnect {
    hostname: String,
    connector: TlsConnector,
}

impl<S> TlsConnect<S> for NativeTlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = NativeTlsStream<S>;
    type Error = native_tls::Error;
    type Future =
        std::pin::Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send + 'static>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let stream = self.connector.connect(&self.hostname, stream).await?;
            Ok(NativeTlsStream(stream))
        })
    }
}

pub struct NativeTlsStream<S>(TlsStream<S>);

impl<S> tokio_postgres::tls::TlsStream for NativeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

impl<S> AsyncRead for NativeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for NativeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
