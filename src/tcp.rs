/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:48
 * @FilePath     : /connect-io/src/tcp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:52:19
 * @Description  : 
 */
use crate::{ConnectionState, Transport, TransportConfig, TransportError};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

/// TCP 传输实现（同步）
///
/// 封装 `std::net::TcpStream`，提供基于 TCP 协议的可靠字节流传输。
/// 支持客户端模式（主动连接）和服务端模式（接受传入连接）。
///
/// # Characteristics
/// - **协议**: TCP（传输控制协议），面向连接、可靠、有序
/// - **模式**: 支持客户端和服务端两种连接方式
/// - **默认超时**: 读/写超时均为 5 秒
/// - **线程安全**: 实现 `Send`，可在多线程间移动所有权
///
/// # Example
/// ```ignore
/// use connect_io::{TcpTransport, Transport, TransportConfig};
/// use std::net::SocketAddr;
///
/// // 客户端模式
/// let addr: SocketAddr = "192.168.1.100:8080".parse().unwrap();
/// let config = TransportConfig::TcpClient { addr };
/// let mut transport = TcpTransport::connect(config)?;
///
/// // 发送数据
/// transport.write_all(b"Hello TCP Server")?;
/// transport.flush()?;
///
/// // 接收数据
/// let mut buf = [0u8; 1024];
/// let n = transport.read(&mut buf)?;
///
/// transport.close()?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
pub struct TcpTransport {
    stream: TcpStream,
}

impl TcpTransport {
    /// 从已有的 TcpStream 创建 TcpTransport 实例
    ///
    /// 供内部组件（如 TcpServerManager）在 accept 后构造实例使用。
    /// 不设置额外超时，调用者应自行配置。
    ///
    /// # Arguments
    /// * `stream` - 已连接的 TCP 流
    pub fn from_stream(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl Transport for TcpTransport {
    /// 建立 TCP 连接
    ///
    /// 根据配置创建 TCP 客户端或服务端连接：
    /// - **TcpClient**: 主动连接到目标服务器地址
    /// - **TcpServer**: 绑定本地端口并等待第一个客户端连接（阻塞式 accept）
    ///
    /// 无论哪种模式，成功后都会设置默认的读/写超时为 5 秒。
    ///
    /// # Arguments
    /// * `config` - 必须为 `TcpClient` 或 `TcpServer` 变体
    ///
    /// # Returns
    /// 成功返回已连接的 `TcpTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected TCP config")` - 配置类型不是 TCP 相关变体
    /// - `Io(ConnectionRefused)` - 目标服务器拒绝连接（仅客户端模式）
    /// - `Io(TimedOut)` - 连接超时（仅客户端模式）
    /// - `Io(AddrInUse)` - 端口已被占用（仅服务端模式）
    ///
    /// # Example
    /// ```ignore
    /// let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    /// let config = TransportConfig::TcpClient { addr };
    /// let mut transport = TcpTransport::connect(config)?;
    /// ```
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

    /// 关闭 TCP 连接
    ///
    /// 执行双向关闭（Shutdown::Both），停止读/写操作。
    /// 底层调用 `TcpStream::shutdown(Both)` 发送 FIN 包。
    ///
    /// # Returns
    /// - `Ok(())` - 成功关闭
    /// - `Err(Io(...))` - socket 已关闭或发生 I/O 错误
    ///
    /// # Note
    /// 调用后底层 socket 仍占用资源直到 `TcpTransport` 被 drop。
    fn close(&mut self) -> Result<(), TransportError> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }

    /// 检查 TCP 连接是否存活
    ///
    /// 通过非阻塞的 `peek` 操作检测 socket 状态。
    /// 如果 peek 成功说明连接仍然有效，失败则表示连接已断开。
    ///
    /// # Returns
    /// - `true` - 连接正常，可进行数据传输
    /// - `false` - 连接已断开或发生错误
    fn is_connected(&self) -> bool {
        self.stream.peek(&mut []).is_ok()
    }

    /// 设置 TCP 读/写超时时间
    ///
    /// 同时设置读取和写入操作的超时时间。
    /// 超时后 pending 的 read/write 会返回 `TimedOut` 错误。
    ///
    /// # Arguments
    /// * `timeout` - 超时时间，None 表示无限等待
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败（如无效的时长值）
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.stream.set_read_timeout(timeout)?;
        self.stream.set_write_timeout(timeout)?;
        Ok(())
    }

    /// 获取 TCP 本地绑定地址
    ///
    /// 返回本端 socket 的 IP 地址和端口号。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址（如 "192.168.1.50:54321"）
    /// - `None` - 获取失败
    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.local_addr().ok()
    }

    /// 获取 TCP 远程对端地址
    ///
    /// 返回已连接的对端 socket 地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址（如 "192.168.1.100:8080"）
    /// - `None` - 未连接或获取失败
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    /// 获取 TCP 连接状态
    ///
    /// 通过 peek 操作检测连接是否存活，返回详细的状态枚举。
    ///
    /// # Returns
    /// - `ConnectionState::Connected` - TCP 连接正常
    /// - `ConnectionState::Disconnected` - 连接已断开或 socket 错误
    fn connection_state(&self) -> ConnectionState {
        if self.stream.peek(&mut []).is_ok() {
            ConnectionState::Connected
        } else {
            ConnectionState::Disconnected
        }
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