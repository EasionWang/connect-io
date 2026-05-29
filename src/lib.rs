/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:31:03
 * @FilePath     : /connect-io/src/lib.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:44:45
 * @Description  : 
 */
mod error;
pub use error::TransportError;

#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "serial")]
pub mod serial;

#[cfg(feature = "async")]
pub mod async_impl;

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

/// 传输配置
#[derive(Debug, Clone)]
pub enum TransportConfig {
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
        data_bits: serialport::DataBits,
        stop_bits: serialport::StopBits,
        parity: serialport::Parity,
        flow_control: serialport::FlowControl,
    },
}

/// 统一传输 Trait（同步）
pub trait Transport: Read + Write {
    fn connect(config: TransportConfig) -> Result<Self, TransportError>
    where
        Self: Sized;
    fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr> { None }
    fn peer_addr(&self) -> Option<SocketAddr> { None }
}

/// 工厂函数
pub fn create_transport(config: TransportConfig) -> Result<Box<dyn Transport>, TransportError> {
    match &config {
        TransportConfig::TcpClient { .. } | TransportConfig::TcpServer { .. } => {
            #[cfg(feature = "tcp")]
            {
                Ok(Box::new(tcp::TcpTransport::connect(config)?))
            }
            #[cfg(not(feature = "tcp"))]
            {
                Err(TransportError::Config("TCP feature not enabled".into()))
            }
        }
        TransportConfig::Udp { .. } => {
            #[cfg(feature = "udp")]
            {
                Ok(Box::new(udp::UdpTransport::connect(config)?))
            }
            #[cfg(not(feature = "udp"))]
            {
                Err(TransportError::Config("UDP feature not enabled".into()))
            }
        }
        #[cfg(feature = "serial")]
        TransportConfig::Serial { .. } => {
            #[cfg(feature = "serial")]
            {
                Ok(Box::new(serial::SerialTransport::connect(config)?))
            }
            #[cfg(not(feature = "serial"))]
            {
                Err(TransportError::Config("Serial feature not enabled".into()))
            }
        }
        #[cfg(not(feature = "serial"))]
        _ => {
            // 这个分支理论上不会到达，因为我们已经在上面用 cfg 过滤了
            Err(TransportError::Config("Unknown config".into()))
        }
    }
}

#[cfg(feature = "async")]
pub use async_impl::{AsyncTransportConfig, AsyncTcpTransport, AsyncUdpTransport, AsyncSerialTransport, create_async_transport};
