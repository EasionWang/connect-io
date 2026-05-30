/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:31:03
 * @FilePath     : /connect-io/src/async_impl/mod.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 14:20:16
 * @Description  : 异步传输模块
 */
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "tcp")]
pub mod tcp_server;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "serial")]
pub mod serial;

use crate::{ConnectionState, TransportError};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "serial")]
use serialport::{DataBits, StopBits, Parity, FlowControl};

/// 异步传输配置枚举
///
/// 定义异步版本的所有支持的传输协议及其配置参数。
/// 与同步版 `TransportConfig` 类似，但用于 `AsyncTransport::connect()` 和
/// `create_async_transport()` 工厂函数。
///
/// # Feature Gates
/// - `TcpClient` / `TcpServer`: 需要 `tcp` feature
/// - `Udp`: 需要 `udp` feature
/// - `Serial`: 需要 `serial` feature
///
/// # Example
/// ```ignore
/// use connect_io::AsyncTransportConfig;
/// use std::net::SocketAddr;
///
/// // 异步 TCP 客户端配置
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = AsyncTransportConfig::TcpClient { addr };
/// ```
#[derive(Debug, Clone)]
pub enum AsyncTransportConfig {
    /// 异步 TCP 客户端连接配置
    ///
    /// 用于创建异步 TCP 客户端，主动连接到目标服务器。
    /// 基于 tokio 的 `TcpStream` 实现。
    ///
    /// # Fields
    /// * `addr` - 目标服务器的 Socket 地址（IP:Port）
    TcpClient { addr: SocketAddr },

    /// 异步 TCP 服务端监听配置
    ///
    /// 用于创建异步 TCP 服务端，绑定端口并等待连接。
    /// 注意：此变体用于单次 accept 场景，如需管理多个连接建议使用 `AsyncTcpServerManager`。
    ///
    /// # Fields
    /// * `bind_addr` - 本地绑定的监听地址
    TcpServer { bind_addr: SocketAddr },

    /// 异步 UDP 传输配置
    ///
    /// 用于创建基于 tokio 的 UDP socket。
    /// 支持有连接和无连接两种模式，与同步版行为一致。
    ///
    /// # Fields
    /// * `bind_addr` - 本地绑定的地址
    /// * `peer_addr` - 可选的对端地址
    Udp {
        bind_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
    },

    /// 异步串口传输配置
    ///
    /// 用于创建基于 tokio-serial 的异步串口连接。
    ///
    /// # Feature
    /// 需要启用 `serial` feature 才可使用此变体。
    ///
    /// # Fields
    /// * `port` - 串口设备路径
    /// * `baud_rate` - 波特率
    /// * `data_bits` - 数据位
    /// * `stop_bits` - 停止位
    /// * `parity` - 校验位
    /// * `flow_control` - 流控制模式
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

/// 统一异步传输 Trait
///
/// 定义异步传输通道的通用接口，支持 TCP、UDP、串口等多种传输协议。
/// 基于 tokio 的 `AsyncRead` + `AsyncWrite` trait，可在 async 上下文中使用。
///
/// 与同步版 `Transport` trait 的主要区别：
/// - 所有连接操作是 `async fn`
/// - 基于 tokio 运行时，不阻塞线程
/// - 需要实现 `Send + Sync` 以支持跨 task 调度
///
/// # Lifecycle
/// 1. 调用 `connect().await` 建立连接（async）
/// 2. 使用 AsyncRead/AsyncWrite 进行数据传输
/// 3. 调用 `close().await` 关闭连接（async）
///
/// # Thread Safety
/// 要求实现 `Send + Sync`，可安全在多个 tokio task 间共享引用。
///
/// # Example
/// ```ignore
/// use connect_io::async_impl::{AsyncTransport, AsyncTcpTransport, AsyncTransportConfig};
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
/// let config = AsyncTransportConfig::TcpClient { addr };
/// let mut transport = AsyncTcpTransport::connect(config).await?;
///
/// // 异步写入数据
/// use tokio::io::AsyncWriteExt;
/// transport.write_all(b"Hello").await?;
/// transport.flush().await?;
///
/// // 异步读取响应
/// use tokio::io::AsyncReadExt;
/// let mut buf = [0u8; 1024];
/// let n = transport.read(&mut buf).await?;
///
/// transport.close().await?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
#[async_trait]
pub trait AsyncTransport: AsyncRead + AsyncWrite + Send + Sync {
    /// 异步建立传输连接
    ///
    /// 创建并初始化异步传输通道。根据配置类型创建对应的传输实例。
    /// 此方法是 async 的，不会阻塞当前线程。
    ///
    /// # Arguments
    /// * `config` - 传输配置，必须与具体实现类型匹配
    ///
    /// # Returns
    /// 成功返回已连接的传输实例，失败返回对应错误
    ///
    /// # Errors
    /// - `Config` - 配置类型不匹配
    /// - `ConnectionFailed` - 连接建立失败（网络不可达、超时等）
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError>
    where
        Self: Sized;

    /// 异步关闭传输连接
    ///
    /// 释放底层资源并关闭传输通道。对于 TCP 会执行 shutdown 操作。
    /// 此方法是 async 的，但通常很快完成。
    ///
    /// # Returns
    /// 成功返回 `Ok(())`，失败返回错误
    async fn close(&mut self) -> Result<(), TransportError>;

    /// 检查当前是否处于已连接状态
    ///
    /// 快速检查传输通道是否可用。不同协议的判断方式不同。
    ///
    /// # Returns
    /// - `true` - 已连接，可进行数据传输
    /// - `false` - 未连接或连接已断开
    fn is_connected(&self) -> bool;

    /// 设置读写超时时间（占位接口）
    ///
    /// 注意：tokio 异步 I/O 通常不使用传统超时机制，
    /// 而是配合 `tokio::time::timeout()` 使用。
    /// 当前实现为空操作（始终返回成功）。
    ///
    /// # Arguments
    /// * `_timeout` - 超时时间（当前未使用）
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    /// 获取本地绑定的地址
    ///
    /// 返回传输层本地绑定的 socket 地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址获取成功
    /// - `None` - 不支持或获取失败
    fn local_addr(&self) -> Option<SocketAddr> { None }

    /// 获取远程对端的地址
    ///
    /// 返回传输层对端的 socket 地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址获取成功
    /// - `None` - 未连接或不支持
    fn peer_addr(&self) -> Option<SocketAddr> { None }

    /// 获取当前连接状态（异步版本）
    ///
    /// 返回详细的连接状态信息，比 `is_connected()` 提供更多语义信息。
    /// 默认实现返回 `Unknown`，各传输类型应覆盖以提供准确状态。
    ///
    /// # Returns
    /// 当前连接状态枚举值：
    /// - `Connected`: 已连接且可用
    /// - `Disconnected`: 已断开
    /// - `Connecting`: 正在连接中
    /// - `Unknown`: 状态未知
    fn connection_state(&self) -> ConnectionState {
        ConnectionState::Unknown
    }
}

#[cfg(feature = "tcp")]
pub use tcp::AsyncTcpTransport;
#[cfg(feature = "tcp")]
pub use tcp_server::AsyncTcpServerManager;
#[cfg(feature = "udp")]
pub use udp::AsyncUdpTransport;
#[cfg(feature = "serial")]
pub use serial::AsyncSerialTransport;

/// 异步传输工厂函数
///
/// 根据 `AsyncTransportConfig` 配置创建对应的异步传输实例，返回 trait 对象。
/// 这是创建异步传输通道的推荐入口点，无需关心具体实现类型。
///
/// 该函数会根据配置自动选择对应的异步传输实现：
/// - `TcpClient` / `TcpServer` → `AsyncTcpTransport`
/// - `Udp` → `AsyncUdpTransport`
/// - `Serial` → `AsyncSerialTransport`
///
/// # Arguments
/// * `config` - 异步传输配置，决定创建的传输类型和参数
///
/// # Returns
/// 成功返回装箱后的 `dyn AsyncTransport` trait 对象，
/// 失败返回对应的错误（配置错误、连接失败等）
///
/// # Errors
/// - `Config("TCP feature not enabled")` - 尝试使用 TCP 但未启用 tcp feature
/// - `Config("UDP feature not enabled")` - 尝试使用 UDP 但未启用 udp feature
/// - `Config("Serial feature not enabled")` - 尝试使用串口但未启用 serial feature
/// - `ConnectionFailed(...)` - 连接建立失败
///
/// # Example
/// ```ignore
/// use connect_io::async_impl::{create_async_transport, AsyncTransport, AsyncTransportConfig};
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = AsyncTransportConfig::TcpClient { addr };
/// let mut transport = create_async_transport(config)?;
///
/// // 使用 transport 进行异步通信...
/// transport.close().await?;
/// ```
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
