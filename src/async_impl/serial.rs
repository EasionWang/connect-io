/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:37
 * @FilePath     : /connect-io/src/async_impl/serial.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:23:11
 * @Description  : 异步串口传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig, ConnectionState};
use crate::TransportError;
use async_trait::async_trait;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serial::SerialPortBuilderExt;

/// 异步串口传输实现
///
/// 封装 `tokio_serial` 库的 `SerialStream`，
/// 提供基于 RS-232/RS-485 串口的异步字节流传输能力。
///
/// # Characteristics
/// - **协议**: 串口通信（物理层点对点）
/// - **运行时**: 基于 tokio，不阻塞线程
/// - **连接模型**: 打开端口即视为已连接（无握手概念）
/// - **数据格式**: 可配置波特率、数据位、停止位、校验位、流控制
///
/// # Platform Support
/// - **Linux**: `/dev/ttyUSB0`, `/dev/ttyACM0`
/// - **macOS**: `/dev/cu.usbserial`, `/dev/tty.usbmodem`
/// - **Windows**: `COM1`, `COM3`
///
/// # Example
/// ```ignore
/// use connect_io::async_impl::{AsyncSerialTransport, AsyncTransport, AsyncTransportConfig};
/// use serialport::{DataBits, StopBits, Parity, FlowControl};
///
/// let config = AsyncTransportConfig::Serial {
///     port: "/dev/ttyUSB0".to_string(),
///     baud_rate: 115200,
///     data_bits: DataBits::Eight,
///     stop_bits: StopBits::One,
///     parity: Parity::None,
///     flow_control: FlowControl::None,
/// };
/// let mut transport = AsyncSerialTransport::connect(config).await?;
///
/// use tokio::io::AsyncWriteExt;
/// transport.write_all(b"AT\r\n").await?;
/// transport.flush().await?;
///
/// transport.close().await?;
/// ```
pub struct AsyncSerialTransport {
    port: tokio_serial::SerialStream,
}

#[async_trait]
impl AsyncTransport for AsyncSerialTransport {
    /// 异步打开串口设备
    ///
    /// 根据配置参数异步打开指定的串口设备并设置通信参数。
    /// 成功后串口即可进行异步数据收发。
    ///
    /// # Arguments
    /// * `config` - 必须为 `Serial` 变体，包含端口路径和通信参数
    ///
    /// # Returns
    /// 成功返回已打开的 `AsyncSerialTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected Serial config")` - 配置类型不是 Serial
    /// - `ConnectionFailed(...)` - 设备不存在、无效参数或权限不足
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

    /// 异步关闭串口
    ///
    /// 串口无显式关闭操作，此方法仅做兼容性处理，始终返回成功。
    /// 底层串口资源在 `AsyncSerialTransport` 被 drop 时自动释放。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    async fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    /// 检查异步串口是否可用
    ///
    /// 串口打开后即视为已连接，始终返回 true。
    ///
    /// # Returns
    /// 始终返回 `true`
    fn is_connected(&self) -> bool {
        true
    }

    /// 设置超时时间（占位实现）
    ///
    /// tokio 异步 I/O 不使用传统超时机制。
    /// 此方法为空操作，始终返回成功。
    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    /// 获取异步串口连接状态
    ///
    /// 串口无连接概念，打开即视为已连接。
    ///
    /// # Returns
    /// 始终返回 `ConnectionState::Connected`
    fn connection_state(&self) -> ConnectionState {
        // 串口无连接概念，打开即视为已连接
        ConnectionState::Connected
    }
}

/// AsyncRead 实现 - 异步读取串口数据
///
/// 委托给内部 `SerialStream::poll_read`。
impl AsyncRead for AsyncSerialTransport {
    /// 异步从串口读取数据
    ///
    /// # Arguments
    /// * `cx` - 异步上下文
    /// * `buf` - 读取缓冲区
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.port).poll_read(cx, buf)
    }
}

/// AsyncWrite 实现 - 异步写入串口数据
///
/// 委托给内部 `SerialStream::poll_write` / `poll_flush` / `poll_shutdown`。
impl AsyncWrite for AsyncSerialTransport {
    /// 异步向串口写入数据
    ///
    /// # Arguments
    /// * `cx` - 异步上下文
    /// * `buf` - 待写入的数据
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
