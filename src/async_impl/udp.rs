/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:54
 * @FilePath     : /connect-io/src/async_impl/udp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:46:45
 * @Description  : 异步 UDP 传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig};
use crate::TransportError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UdpSocket;

pub struct AsyncUdpTransport {
    socket: UdpSocket,
    connected: bool,
}

#[async_trait]
impl AsyncTransport for AsyncUdpTransport {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError> {
        if let AsyncTransportConfig::Udp { bind_addr, peer_addr } = config {
            let socket = UdpSocket::bind(bind_addr).await
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

            let connected = if let Some(peer) = peer_addr {
                socket.connect(peer).await
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
                true
            } else {
                false
            };

            Ok(Self { socket, connected })
        } else {
            Err(TransportError::Config("Expected UDP config".into()))
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.local_addr().ok()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.peer_addr().ok()
    }
}

impl AsyncRead for AsyncUdpTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.connected {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            )));
        }

        let socket = &this.socket;
        let mut recv_buf = vec![0u8; buf.remaining()];
        match socket.poll_recv(cx, &mut recv_buf) {
            std::task::Poll::Ready(Ok(n)) => {
                buf.put_slice(&recv_buf[..n]);
                std::task::Poll::Ready(Ok(()))
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl AsyncWrite for AsyncUdpTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();
        if !this.connected {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            )));
        }

        let socket = &this.socket;
        match socket.poll_send(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => std::task::Poll::Ready(Ok(n)),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
