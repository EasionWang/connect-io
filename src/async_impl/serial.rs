/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:37
 * @FilePath     : /connect-io/src/async_impl/serial.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:47:07
 * @Description  : 异步串口传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig};
use crate::TransportError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serial::SerialPortBuilderExt;

pub struct AsyncSerialTransport {
    port: tokio_serial::SerialStream,
}

#[async_trait]
impl AsyncTransport for AsyncSerialTransport {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError> {
        if let AsyncTransportConfig::Serial {
            port,
            baud_rate,
            data_bits,
            stop_bits,
            parity,
            flow_control,
        } = config {
            let builder = tokio_serial::new(&port, baud_rate)
                .data_bits(data_bits)
                .stop_bits(stop_bits)
                .parity(parity)
                .flow_control(flow_control);
            let port = builder.open_native_async()
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
            Ok(AsyncSerialTransport { port })
        } else {
            Err(TransportError::Config("Expected Serial config".into()))
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }
}

impl AsyncRead for AsyncSerialTransport {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.port).poll_read(cx, buf)
    }
}

impl AsyncWrite for AsyncSerialTransport {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.port).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.port).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.port).poll_shutdown(cx)
    }
}
