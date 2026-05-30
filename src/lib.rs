/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:31:03
 * @FilePath     : /connect-io/src/lib.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:12:39
 * @Description  : 
 */
mod error;
pub use error::TransportError;

/// 连接状态枚举
///
/// 描述传输通道的当前连接状态，用于区分已连接、断开、连接中、未知等状态。
/// 该枚举实现了 `Debug`、`Clone`、`PartialEq`、`Eq` trait，可安全跨线程传递和比较。
///
/// # Variants
/// - `Connected`: 已建立稳定连接，可进行数据收发
/// - `Disconnected`: 已断开连接，需要重新建立连接才能通信
/// - `Connecting`: 正在尝试建立连接中（握手阶段）
/// - `Unknown`: 状态未知，通常作为默认初始状态
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// 已建立连接
    ///
    /// 表示传输通道已成功建立，可以进行正常的数据读写操作。
    /// 对于 TCP 意味着三次握手完成；对于串口意味着端口已打开；
    /// 对于 UDP 意味着 socket 已绑定并可选地 connect 到对端。
    Connected,

    /// 已断开连接
    ///
    /// 表示传输通道已关闭或连接丢失。
    /// 可能由主动关闭、网络中断、超时等原因导致。
    Disconnected,

    /// 正在建立连接中
    ///
    /// 表示正在进行连接建立过程（如 TCP 三次握手），
    /// 尚未完成，此时不应进行数据传输操作。
    Connecting,

    /// 状态未知（默认值）
    ///
    /// 初始默认状态或无法确定当前连接状态时使用。
    /// 各传输实现应尽可能覆盖此默认值以提供准确状态。
    Unknown,
}

#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(feature = "tcp")]
pub mod tcp_server;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "serial")]
pub mod serial;

#[cfg(feature = "async")]
pub mod async_impl;

#[cfg(feature = "tauri")]
pub mod tauri_plugin;

#[cfg(feature = "tauri")]
pub use tauri_plugin::TransportState;

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

/// 传输配置枚举
///
/// 定义所有支持的传输协议及其配置参数。
/// 该枚举作为 `Transport::connect()` 和 `create_transport()` 工厂函数的输入，
/// 用于指定要创建的传输类型和连接参数。
///
/// # Feature Gates
/// - `TcpClient` / `TcpServer`: 需要 `tcp` feature
/// - `Udp`: 需要 `udp` feature
/// - `Serial`: 需要 `serial` feature
///
/// # Example
/// ```ignore
/// use std::net::SocketAddr;
/// use connect_io::TransportConfig;
///
/// // TCP 客户端配置
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = TransportConfig::TcpClient { addr };
///
/// // UDP 配置（带默认对端）
/// let bind_addr: SocketAddr = "0.0.0.0:9000".parse().unwrap();
/// let peer_addr: SocketAddr = "192.168.1.100:9000".parse().unwrap();
/// let udp_config = TransportConfig::Udp {
///     bind_addr,
///     peer_addr: Some(peer_addr),
/// };
/// ```
#[derive(Debug, Clone)]
pub enum TransportConfig {
    /// TCP 客户端连接配置
    ///
    /// 用于创建主动发起连接的 TCP 客户端。
    /// 连接建立后可进行双向字节流通信。
    ///
    /// # Fields
    /// * `addr` - 目标服务器的 Socket 地址（IP:Port）
    TcpClient { addr: SocketAddr },

    /// TCP 服务端监听配置
    ///
    /// 用于创建 TCP 服务端监听器，等待客户端连接。
    /// 注意：此变体用于 `TcpTransport::connect()` 中的服务端模式，
    /// 如需管理多个连接建议使用 `TcpServerManager`。
    ///
    /// # Fields
    /// * `bind_addr` - 本地绑定的监听地址
    TcpServer { bind_addr: SocketAddr },

    /// UDP 传输配置
    ///
    /// 用于创建 UDP socket，支持有连接和无连接两种模式：
    /// - 指定 `peer_addr` 时为已连接模式，可直接使用 read/write
    /// - 不指定 `peer_addr` 时为无连接模式，需使用 send_to/recv_from
    ///
    /// # Fields
    /// * `bind_addr` - 本地绑定的地址
    /// * `peer_addr` - 可选的对端地址，None 表示无连接模式
    Udp {
        bind_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
    },

    /// 串口传输配置
    ///
    /// 用于创建串口（RS-232/RS-485）连接。
    /// 串口是点对点通信，打开端口即视为已连接。
    ///
    /// # Feature
    /// 需要启用 `serial` feature 才可使用此变体。
    ///
    /// # Fields
    /// * `port` - 串口设备路径（如 "/dev/ttyUSB0" 或 "COM3"）
    /// * `baud_rate` - 波特率（如 9600、115200）
    /// * `data_bits` - 数据位（通常为 8）
    /// * `stop_bits` - 停止位
    /// * `parity` - 校验位
    /// * `flow_control` - 流控制模式
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

/// 统一传输 Trait（同步版本）
///
/// 定义同步传输通道的通用接口，支持 TCP、UDP、串口等多种传输协议。
/// 实现此 trait 的类型必须同时实现 `std::io::Read` 和 `std::io::Write`，
/// 以提供标准的字节流读写能力。
///
/// # Lifecycle
/// 1. 调用 `connect()` 建立连接
/// 2. 使用 `read()`/`write()` 进行数据传输
/// 3. 调用 `close()` 关闭连接
///
/// # Thread Safety
/// 实现 `Send` 即可在多线程间移动所有权（`&mut self` 方法需要外部同步）。
///
/// # Example
/// ```ignore
/// use connect_io::{Transport, TransportConfig, TcpTransport};
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
/// let config = TransportConfig::TcpClient { addr };
/// let mut transport = TcpTransport::connect(config)?;
///
/// // 写入数据
/// transport.write_all(b"Hello")?;
/// transport.flush()?;
///
/// // 读取响应
/// let mut buf = [0u8; 1024];
/// let n = transport.read(&mut buf)?;
///
/// transport.close()?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
pub trait Transport: Read + Write {
    /// 根据配置建立传输连接
    ///
    /// 创建并初始化传输通道。对于 TCP 客户端会发起连接到目标服务器；
    /// 对于 TCP 服务端会绑定端口并等待第一个客户端连接；
    /// 对于 UDP 会绑定本地 socket；对于串口会打开指定端口。
    ///
    /// # Arguments
    /// * `config` - 传输配置，必须与具体实现类型匹配
    ///
    /// # Returns
    /// 成功返回已连接的传输实例，失败返回对应错误
    ///
    /// # Errors
    /// - `Config` - 配置类型不匹配（如用 TCP 配置创建 UDP 传输）
    /// - `ConnectionFailed` - 连接建立失败（网络不可达、端口被占用等）
    /// - `Io` - 底层 I/O 错误（权限不足、设备不存在等）
    fn connect(config: TransportConfig) -> Result<Self, TransportError>
    where
        Self: Sized;

    /// 关闭传输连接
    ///
    /// 释放底层资源并关闭传输通道。
    /// 对于 TCP 会发送 FIN 包执行优雅关闭；
    /// 对于 UDP 和串口仅释放资源。
    ///
    /// # Returns
    /// 成功返回 `Ok(())`，失败返回错误
    ///
    /// # Note
    /// 调用后对象不应再用于数据传输，但可以重新调用 `connect()` 建立新连接（如果实现支持）。
    fn close(&mut self) -> Result<(), TransportError>;

    /// 检查当前是否处于已连接状态
    ///
    /// 快速检查传输通道是否可用。
    /// 不同协议的判断方式不同：
    /// - TCP: 通过 peek 操作检测 socket 是否存活
    /// - UDP: 检查是否已调用 connect()
    /// - 串口: 始终返回 true（打开即视为连接）
    ///
    /// # Returns
    /// - `true` - 已连接，可进行数据传输
    /// - `false` - 未连接或连接已断开
    fn is_connected(&self) -> bool;

    /// 设置读写超时时间
    ///
    /// 配置后续所有 read/write 操作的超时时间。
    /// 超时后操作会返回 `TimedOut` 错误。
    ///
    /// # Arguments
    /// * `timeout` - 超时时间，None 表示无限等待，Some(duration) 表示指定超时
    ///
    /// # Returns
    /// 成功返回 `Ok(())`，失败返回错误
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// // 设置 10 秒超时
    /// transport.set_timeout(Some(Duration::from_secs(10)))?;
    ///
    /// // 取消超时（阻塞等待）
    /// transport.set_timeout(None)?;
    /// ```
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;

    /// 获取本地绑定的地址
    ///
    /// 返回传输层本地绑定的 socket 地址。
    /// 对于服务端模式返回监听地址；对于客户端返回操作系统分配的本地地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址获取成功
    /// - `None` - 不支持或获取失败（如某些虚拟传输）
    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// 获取远程对端的地址
    ///
    /// 返回传输层对端的 socket 地址。
    /// 仅在已连接模式下有意义（TCP 已连接、UDP 已 connect）。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址获取成功
    /// - `None` - 未连接或不支持
    fn peer_addr(&self) -> Option<SocketAddr> {
        None
    }

    /// 获取当前连接状态
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

/// 传输工厂函数（同步版本）
///
/// 根据 `TransportConfig` 配置创建对应的传输实例，返回 trait 对象。
/// 这是创建传输通道的推荐入口点，无需关心具体实现类型。
///
/// 该函数会根据配置自动选择对应的传输实现：
/// - `TcpClient` / `TcpServer` → `TcpTransport`
/// - `Udp` → `UdpTransport`
/// - `Serial` → `SerialTransport`
///
/// # Arguments
/// * `config` - 传输配置，决定创建的传输类型和参数
///
/// # Returns
/// 成功返回装箱后的 `dyn Transport` trait 对象，
/// 失败返回对应的错误（配置错误、连接失败、I/O 错误等）
///
/// # Errors
/// - `Config("TCP feature not enabled")` - 尝试使用 TCP 但未启用 tcp feature
/// - `Config("UDP feature not enabled")` - 尝试使用 UDP 但未启用 udp feature
/// - `Config("Serial feature not enabled")` - 尝试使用串口但未启用 serial feature
/// - `ConnectionFailed(...)` - 连接建立失败
/// - `Io(...)` - 底层 I/O 错误
///
/// # Example
/// ```ignore
/// use connect_io::{create_transport, Transport, TransportConfig};
/// use std::net::SocketAddr;
///
/// // 创建 TCP 客户端
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = TransportConfig::TcpClient { addr };
/// let mut transport = create_transport(config)?;
///
/// // 使用 transport 进行通信...
/// transport.write_all(b"Hello World")?;
/// transport.close()?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
///
/// # Feature Gates
/// 各传输类型需要对应 feature 启用：
/// - TCP: `tcp`
/// - UDP: `udp`
/// - Serial: `serial`
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
pub use async_impl::{AsyncTransportConfig, AsyncTcpTransport, AsyncUdpTransport, AsyncSerialTransport, AsyncTcpServerManager, create_async_transport};

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    // ============================================================
    // TransportConfig 构造验证
    // ============================================================

    #[test]
    fn FunEvent_config_tcp_client_constructs() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().expect("parse addr");
        let config = TransportConfig::TcpClient { addr };

        match config {
            TransportConfig::TcpClient { addr: a } => {
                assert_eq!(a, "127.0.0.1:8080".parse::<SocketAddr>().unwrap());
            }
            _ => panic!("expected TcpClient variant"),
        }
    }

    #[test]
    fn FunEvent_config_tcp_server_constructs() {
        let addr: SocketAddr = "0.0.0.0:9000".parse().expect("parse addr");
        let config = TransportConfig::TcpServer { bind_addr: addr };

        match config {
            TransportConfig::TcpServer { bind_addr } => {
                assert_eq!(bind_addr.port(), 9000);
            }
            _ => panic!("expected TcpServer variant"),
        }
    }

    #[test]
    fn FunEvent_config_udp_with_peer_constructs() {
        let bind: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let peer: SocketAddr = "192.168.1.1:5000".parse().unwrap();
        let config = TransportConfig::Udp {
            bind_addr: bind,
            peer_addr: Some(peer),
        };

        match config {
            TransportConfig::Udp {
                bind_addr,
                peer_addr,
            } => {
                assert_eq!(bind_addr.port(), 0);
                assert_eq!(peer_addr, Some(peer));
            }
            _ => panic!("expected Udp variant"),
        }
    }

    #[test]
    fn FunEvent_config_udp_without_peer_constructs() {
        let bind: SocketAddr = "0.0.0.0:9999".parse().unwrap();
        let config = TransportConfig::Udp {
            bind_addr: bind,
            peer_addr: None,
        };

        match config {
            TransportConfig::Udp { peer_addr, .. } => {
                assert!(peer_addr.is_none(), "peer_addr should be None");
            }
            _ => panic!("expected Udp variant"),
        }
    }

    // ============================================================
    // 工厂函数 feature gate 测试
    // ============================================================

    #[cfg(not(feature = "tcp"))]
    #[test]
    fn FunEvent_factory_tcp_disabled_returns_error() {
        let config = TransportConfig::TcpClient {
            addr: "127.0.0.1:8080".parse().unwrap(),
        };
        let result = create_transport(config);
        assert!(result.is_err(), "factory should return error when TCP disabled");
        match result.unwrap_err() {
            TransportError::Config(msg) => {
                assert!(
                    msg.contains("TCP feature not enabled"),
                    "error message should mention feature disabled, got: {}",
                    msg
                );
            }
            other => panic!("expected Config error, got: {:?}", other),
        }
    }

    #[cfg(not(feature = "udp"))]
    #[test]
    fn FunEvent_factory_udp_disabled_returns_error() {
        let config = TransportConfig::Udp {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            peer_addr: None,
        };
        let result = create_transport(config);
        assert!(result.is_err(), "factory should return error when UDP disabled");
        match result.unwrap_err() {
            TransportError::Config(msg) => {
                assert!(
                    msg.contains("UDP feature not enabled"),
                    "error message should mention feature disabled"
                );
            }
            other => panic!("expected Config error, got: {:?}", other),
        }
    }

    // ============================================================
    // TransportError 变体覆盖
    // ============================================================

    #[test]
    fn FunEvent_error_config_variant() {
        let err = TransportError::Config("test config error".into());
        let msg = format!("{}", err);
        assert!(
            msg.contains("Invalid configuration"),
            "Config error Display should contain 'Invalid configuration', got: {}",
            msg
        );
        assert!(
            msg.contains("test config error"),
            "Config error Display should contain original message"
        );
    }

    #[test]
    fn FunEvent_error_connection_failed_variant() {
        let err = TransportError::ConnectionFailed("peer refused".into());
        let msg = format!("{}", err);
        assert!(
            msg.contains("Connection failed"),
            "ConnectionFailed Display should contain prefix"
        );
        assert!(
            msg.contains("peer refused"),
            "ConnectionFailed Display should contain detail"
        );
    }

    #[test]
    fn FunEvent_error_io_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let transport_err = TransportError::from(io_err);
        let msg = format!("{}", transport_err);
        assert!(
            msg.contains("IO error"),
            "Io variant Display should contain 'IO error'"
        );
        assert!(
            msg.contains("pipe broken"),
            "Io variant Display should contain original io message"
        );
    }

    // ============================================================
    // Transport trait 默认方法验证
    // ============================================================

    #[test]
    fn FunEvent_transport_default_methods_exist() {
        // 验证 trait 对象可以调用默认实现（编译期检查）
        // 这里仅验证 trait bound 可以满足，不实际构造对象
        fn _assert_trait_bounds<T: Transport>() {}

        // 编译通过即说明 trait 方法存在
        _assert_trait_bounds::<tcp::TcpTransport>();
        #[cfg(feature = "udp")]
        _assert_trait_bounds::<udp::UdpTransport>();
    }
}

#[cfg(feature = "tcp")]
pub use tcp_server::TcpServerManager;
