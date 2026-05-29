/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:31:03
 * @FilePath     : /connect-io/src/async_impl/mod.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:45:22
 * @Description  : 异步传输模块
 */
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "serial")]
pub mod serial;

use crate::TransportError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "serial")]
use serialport::{DataBits, StopBits, Parity, FlowControl};

#[derive(Debug, Clone)]
pub enum AsyncTransportConfig {
    TcpClient { addr: SocketAddr },
    TcpServer { bind_addr: SocketAddr },
    Udp {
        bind_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
    },
    #[cfg(feature = "serial")]
    Serial {
        port: String,
        baud_rate: u32,
        data_bits: DataBits,
        stop_bits: StopBits,
        parity: Parity,
        flow_control: FlowControl,
    },
}

#[async_trait]
pub trait AsyncTransport: AsyncRead + AsyncWrite + Send + Sync {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError>
    where
        Self: Sized;
    async fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr> { None }
    fn peer_addr(&self) -> Option<SocketAddr> { None }
}

#[cfg(feature = "tcp")]
pub use tcp::AsyncTcpTransport;
#[cfg(feature = "udp")]
pub use udp::AsyncUdpTransport;
#[cfg(feature = "serial")]
pub use serial::AsyncSerialTransport;

pub fn create_async_transport(
    config: AsyncTransportConfig,
) -> Result<Box<dyn AsyncTransport>, TransportError> {
    match &config {
        #[cfg(feature = "tcp")]
        AsyncTransportConfig::TcpClient { .. } | AsyncTransportConfig::TcpServer { .. } => {
            let transport = AsyncTcpTransport::connect(config)?;
            Ok(Box::new(transport))
        }
        #[cfg(feature = "udp")]
        AsyncTransportConfig::Udp { .. } => {
            let transport = AsyncUdpTransport::connect(config)?;
            Ok(Box::new(transport))
        }
        #[cfg(feature = "serial")]
        AsyncTransportConfig::Serial { .. } => {
            let transport = AsyncSerialTransport::connect(config)?;
            Ok(Box::new(transport))
        }
        #[cfg(not(feature = "tcp"))]
        AsyncTransportConfig::TcpClient { .. } | AsyncTransportConfig::TcpServer { .. } => {
            Err(TransportError::Config("TCP feature not enabled".into()))
        }
        #[cfg(not(feature = "udp"))]
        AsyncTransportConfig::Udp { .. } => {
            Err(TransportError::Config("UDP feature not enabled".into()))
        }
        #[cfg(not(feature = "serial"))]
        AsyncTransportConfig::Serial { .. } => {
            Err(TransportError::Config("Serial feature not enabled".into()))
        }
    }
}
