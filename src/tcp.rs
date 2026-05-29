/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:48
 * @FilePath     : /connect-io/src/tcp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:32:50
 * @Description  : 
 */
use crate::{Transport, TransportConfig, TransportError};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

pub struct TcpTransport {
    stream: TcpStream,
}

impl Transport for TcpTransport {
    fn connect(config: TransportConfig) -> Result<Self, TransportError> {
        let stream = match config {
            TransportConfig::TcpClient { addr } => {
                let s = TcpStream::connect(addr)?;
                s.set_read_timeout(Some(Duration::from_secs(5)))?;
                s.set_write_timeout(Some(Duration::from_secs(5)))?;
                s
            }
            TransportConfig::TcpServer { bind_addr } => {
                let listener = TcpListener::bind(bind_addr)?;
                let (s, _) = listener.accept()?;
                s.set_read_timeout(Some(Duration::from_secs(5)))?;
                s.set_write_timeout(Some(Duration::from_secs(5)))?;
                s
            }
            _ => return Err(TransportError::Config("Expected TCP config".into())),
        };
        Ok(TcpTransport { stream })
    }

    fn close(&mut self) -> Result<(), TransportError> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.stream.peek(&mut []).is_ok()
    }

    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.stream.set_read_timeout(timeout)?;
        self.stream.set_write_timeout(timeout)?;
        Ok(())
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.local_addr().ok()
    }

    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }
}

impl Read for TcpTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for TcpTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush()
    }
}