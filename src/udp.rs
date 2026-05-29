/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:54
 * @FilePath     : /connect-io/src/udp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:32:55
 * @Description  : 
 */
use crate::{Transport, TransportConfig, TransportError};
use std::io::{Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

pub struct UdpTransport {
    socket: UdpSocket,
    connected: bool,
}

impl Transport for UdpTransport {
    fn connect(config: TransportConfig) -> Result<Self, TransportError> {
        if let TransportConfig::Udp { bind_addr, peer_addr } = config {
            let socket = UdpSocket::bind(bind_addr)?;
            socket.set_read_timeout(Some(Duration::from_secs(5)))?;
            socket.set_write_timeout(Some(Duration::from_secs(5)))?;

            let connected = if let Some(peer) = peer_addr {
                socket.connect(peer)?;
                true
            } else {
                false
            };

            Ok(UdpTransport { socket, connected })
        } else {
            Err(TransportError::Config("Expected UDP config".into()))
        }
    }

    fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.socket.set_read_timeout(timeout)?;
        self.socket.set_write_timeout(timeout)?;
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.local_addr().ok()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.peer_addr().ok()
    }
}

impl Read for UdpTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.connected {
            self.socket.recv(buf)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            ))
        }
    }
}

impl Write for UdpTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.connected {
            self.socket.send(buf)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            ))
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}