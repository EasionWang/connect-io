/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:48
 * @FilePath     : /connect-io/src/async_impl/tcp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:17:19
 * @Description  : 异步 TCP 传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig, ConnectionState};
use crate::TransportError;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};

/// 异步 TCP 传输实现
///
/// 封装 tokio 的 `TcpStream`，提供基于 TCP 协议的异步可靠字节流传输。
/// 支持客户端模式（主动连接）和服务端模式（接受传入连接）。
///
/// # Characteristics
/// - **协议**: TCP（传输控制协议），面向连接、可靠、有序
/// - **运行时**: 基于 tokio，不阻塞线程
/// - **模式**: 支持客户端和服务端两种连接方式
/// - **线程安全**: 实现 `Send + Sync`，可在多个 tokio task 间共享
///
/// # Internal State
/// 内部使用 `Option<TcpStream>` 持有连接：
/// - `Some(stream)`: 已连接状态
/// - `None`: 未连接或已关闭状态
///
/// # Example
/// ```ignore
/// use connect_io::async_impl::{AsyncTcpTransport, AsyncTransport, AsyncTransportConfig};
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = AsyncTransportConfig::TcpClient { addr };
/// let mut transport = AsyncTcpTransport::connect(config).await?;
///
/// use tokio::io::AsyncWriteExt;
/// transport.write_all(b"Hello Async TCP").await?;
/// transport.flush().await?;
///
/// transport.close().await?;
/// ```
pub struct AsyncTcpTransport {
    stream: Option<TcpStream>,
}

impl AsyncTcpTransport {
    /// 创建未连接的 AsyncTcpTransport 实例
    ///
    /// 返回一个处于未连接状态的实例，stream 为 None。
    /// 通常用于需要延迟初始化的场景，一般建议直接使用 `connect()` 创建已连接的实例。
    ///
    /// # Returns
    /// 未连接的 `AsyncTcpTransport` 实例
    pub fn new() -> Self {
        Self { stream: None }
    }
}

#[async_trait]
impl AsyncTransport for AsyncTcpTransport {
    /// 异步建立 TCP 连接
    ///
    /// 根据配置创建异步 TCP 客户端或服务端连接：
    /// - **TcpClient**: 异步连接到目标服务器（使用 `tokio::net::TcpStream::connect`）
    /// - **TcpServer**: 绑定端口并异步等待第一个客户端连接
    ///
    /// # Arguments
    /// * `config` - 必须为 `TcpClient` 或 `TcpServer` 变体
    ///
    /// # Returns
    /// 成功返回已连接的 `AsyncTcpTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected TCP config")` - 配置类型不是 TCP 相关变体
    /// - `ConnectionFailed(...)` - 连接失败（网络不可达、超时、端口被占用等）
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

    /// 异步关闭 TCP 连接
    ///
    /// 执行 shutdown 操作并释放底层 stream。
    /// 调用后 stream 被设为 None，后续读写操作会返回 `NotConnected` 错误。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`（shutdown 失败被静默忽略）
    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown();
        }
        Ok(())
    }

    /// 检查异步 TCP 是否已连接
    ///
    /// 通过检查内部 `Option<TcpStream>` 是否为 `Some` 判断连接状态。
    /// 注意：这不代表网络连接仍然存活，仅表示尚未调用 close()。
    ///
    /// # Returns
    /// - `true` - stream 存在（可能已连接）
    /// - `false` - 未连接或已关闭
    fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    /// 设置超时时间（占位实现）
    ///
    /// tokio 异步 I/O 不使用传统超时机制。
    /// 此方法为空操作，始终返回成功。
    /// 建议使用 `tokio::time::timeout()` 包装异步操作。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    /// 获取异步 TCP 本地绑定地址
    ///
    /// 返回本端 socket 的 IP 地址和端口号。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址
    /// - `None` - 未连接或获取失败
    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.as_ref().and_then(|s| s.local_addr().ok())
    }

    /// 获取异步 TCP 远程对端地址
    ///
    /// 返回已连接的对端 socket 地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址
    /// - `None` - 未连接或获取失败
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.as_ref().and_then(|s| s.peer_addr().ok())
    }

    /// 获取异步 TCP 连接状态
    ///
    /// 根据内部 stream 是否存在返回对应状态。
    ///
    /// # Returns
    /// - `ConnectionState::Connected` - stream 存在
    /// - `ConnectionState::Disconnected` - stream 为 None
    fn connection_state(&self) -> ConnectionState {
        if self.stream.is_some() {
            ConnectionState::Connected
        } else {
            ConnectionState::Disconnected
        }
    }
}

/// AsyncRead 实现 - 异步读取 TCP 数据
///
/// 委托给内部 `TcpStream::poll_read`。
/// 如果 stream 为 None（未连接），返回 `NotConnected` 错误。
impl AsyncRead for AsyncTcpTransport {
    /// 异步从 TCP 流中读取数据
    ///
    /// # Arguments
    /// * `cx` - 异步上下文
    /// * `buf` - 读取缓冲区
    ///
    /// # Returns
    /// - `Poll::Ready(Ok(()))` - 读取成功（可能读到 EOF）
    /// - `Poll::Ready(Err(...))` - 读取错误或未连接
    /// - `Poll::Pending` - 暂无数据可用，需要等待
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

/// AsyncWrite 实现 - 异步写入 TCP 数据
///
/// 委托给内部 `TcpStream::poll_write` / `poll_flush` / `poll_shutdown`。
/// 如果 stream 为 None（未连接），write 返回 `NotConnected` 错误，
/// flush 和 shutdown 返回 Ok(())。
impl AsyncWrite for AsyncTcpTransport {
    /// 异步向 TCP 流中写入数据
    ///
    /// # Arguments
    /// * `cx` - 异步上下文
    /// * `buf` - 待写入的数据
    ///
    /// # Returns
    /// - `Poll::Ready(Ok(n))` - 成功写入 n 字节
    /// - `Poll::Ready(Err(...))` - 写入错误或未连接
    /// - `Poll::Pending` - 发送缓冲区满，需要等待
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
