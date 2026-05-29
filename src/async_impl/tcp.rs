/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:48
 * @FilePath     : /connect-io/src/async_impl/tcp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:32:50
 * @Description  : 异步 TCP 传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig};
use crate::TransportError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

pub struct AsyncTcpTransport {
    stream: Option<TcpStream>,
}

impl AsyncTcpTransport {
    pub fn new() -> Self {
        Self { stream: None }
    }
}

#[async_trait]
impl AsyncTransport for AsyncTcpTransport {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError> {
        match config {
            AsyncTransportConfig::TcpClient { addr } => {
                let stream = TcpStream::connect(addr).await
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
                Ok(Self { stream: Some(stream) })
            }
            AsyncTransportConfig::TcpServer { bind_addr } => {
                let listener = TcpListener::bind(&bind_addr).await
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
                let (stream, _) = listener.accept().await
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
                Ok(Self { stream: Some(stream) })
            }
            _ => Err(TransportError::Config("Expected TCP config".into())),
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown();
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.as_ref().and_then(|s| s.local_addr().ok())
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.as_ref().and_then(|s| s.peer_addr().ok())
    }
}

impl AsyncRead for AsyncTcpTransport {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            None => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Not connected",
            ))),
        }
    }
}

impl AsyncWrite for AsyncTcpTransport {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            None => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Not connected",
            ))),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}
