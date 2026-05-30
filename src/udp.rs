/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:54
 * @FilePath     : /connect-io/src/udp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 14:14:29
 * @Description  : 
 */
use crate::{ConnectionState, Transport, TransportConfig, TransportError};
use std::io::{Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

/// UDP 传输实现（同步）
///
/// 封装 `std::net::UdpSocket`，提供基于 UDP 协议的数据报传输。
/// 支持有连接模式（connect 到对端后使用 read/write）和无连接模式
/// （使用 send_to/recv_from 指定目标地址）。
///
/// # Characteristics
/// - **协议**: UDP（用户数据报协议），无连接、不可靠、无序
/// - **模式**: 支持已连接模式和无连接模式两种使用方式
/// - **默认超时**: 读/写超时均为 5 秒
/// - **数据边界**: 保持消息边界，每次 recv 对应一次 send
///
/// # Usage Modes
/// 1. **已连接模式**: 配置时指定 peer_addr，之后可直接用 read/write
/// 2. **无连接模式**: 不指定 peer_addr，必须用 send_to/recv_from
///
/// # Example (Connected Mode)
/// ```ignore
/// use connect_io::{UdpTransport, Transport, TransportConfig};
/// use std::net::SocketAddr;
///
/// let bind: SocketAddr = "0.0.0.0:9000".parse().unwrap();
/// let peer: SocketAddr = "192.168.1.100:9000".parse().unwrap();
/// let config = TransportConfig::Udp {
///     bind_addr: bind,
///     peer_addr: Some(peer),
/// };
/// let mut transport = UdpTransport::connect(config)?;
/// transport.write_all(b"Hello UDP")?;
/// ```
///
/// # Example (Unconnected Mode)
/// ```ignore
/// use connect_io::{UdpTransport, Transport, TransportConfig};
/// use std::net::SocketAddr;
///
/// let bind: SocketAddr = "0.0.0.0:9999".parse().unwrap();
/// let config = TransportConfig::Udp { bind_addr: bind, peer_addr: None };
/// let transport = UdpTransport::connect(config)?;
///
/// // 使用 send_to / recv_from
/// transport.send_to(b"Hello", "192.168.1.100:9999".parse().unwrap())?;
/// let mut buf = [0u8; 1024];
/// let (n, addr) = transport.recv_from(&mut buf)?;
/// ```
pub struct UdpTransport {
    socket: UdpSocket,
    connected: bool,
}

impl Transport for UdpTransport {
    /// 建立 UDP socket
    ///
    /// 绑定本地地址并可选地连接到对端。
    /// 如果指定了 peer_addr，socket 会进入已连接模式，
    /// 之后可直接使用标准的 read/write 接口；
    /// 否则处于无连接模式，需使用 send_to/recv_from。
    ///
    /// # Arguments
    /// * `config` - 必须为 `Udp` 变体
    ///
    /// # Returns
    /// 成功返回已初始化的 `UdpTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected UDP config")` - 配置类型不是 Udp
    /// - `Io(AddrInUse)` - 本地绑定地址已被占用
    /// - `Io(...)` - 其他 socket 操作错误
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

    /// 关闭 UDP socket
    ///
    /// UDP 无连接概念，此方法仅做兼容性处理，始终返回成功。
    /// 底层 socket 资源在 drop 时自动释放。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    /// 检查 UDP 是否处于已连接模式
    ///
    /// 返回 socket 是否已通过 `connect()` 绑定到对端地址。
    /// 注意：这不代表网络可达，仅表示本地 socket 已配置对端。
    ///
    /// # Returns
    /// - `true` - 已连接模式（可使用 read/write）
    /// - `false` - 无连接模式（需使用 send_to/recv_from）
    fn is_connected(&self) -> bool {
        self.connected
    }

    /// 设置 UDP 读/写超时时间
    ///
    /// 同时设置接收和发送操作的超时时间。
    /// 超时后 pending 的 recv/send 会返回 `TimedOut` 错误。
    ///
    /// # Arguments
    /// * `timeout` - 超时时间，None 表示无限等待
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.socket.set_read_timeout(timeout)?;
        self.socket.set_write_timeout(timeout)?;
        Ok(())
    }

    /// 获取 UDP 本地绑定地址
    ///
    /// 返回本端 socket 的 IP 地址和端口号。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址
    /// - `None` - 获取失败
    fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.local_addr().ok()
    }

    /// 获取 UDP 远程对端地址
    ///
    /// 仅在已连接模式下返回有效地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址（仅已连接模式）
    /// - `None` - 无连接模式或获取失败
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.peer_addr().ok()
    }

    /// 获取 UDP 连接状态
    ///
    /// 根据是否已 connect 到对端返回对应状态。
    ///
    /// # Returns
    /// - `ConnectionState::Connected` - 已连接模式
    /// - `ConnectionState::Disconnected` - 无连接模式
    fn connection_state(&self) -> ConnectionState {
        if self.connected {
            ConnectionState::Connected
        } else {
            ConnectionState::Disconnected
        }
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
                "UDP socket not connected, use send_to() instead",
            ))
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl UdpTransport {
    /// 无连接模式下发送数据到指定地址
    ///
    /// 将数据报发送到指定的目标地址，无需预先 connect。
    /// 此方法在已连接和无连接模式下均可使用。
    ///
    /// # Arguments
    /// * `buf` - 待发送的数据缓冲区
    /// * `addr` - 目标 Socket 地址（IP:Port）
    ///
    /// # Returns
    /// - `Ok(usize)` - 实际发送的字节数（可能小于 buf.len()）
    /// - `Err(Io(...))` - 发送失败（网络不可达、缓冲区满等）
    ///
    /// # Note
    /// UDP 不保证可靠传输，发送成功不代表对端收到数据。
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, TransportError> {
        let n = self.socket.send_to(buf, addr)?;
        Ok(n)
    }

    /// 无连接模式下从任意地址接收数据
    ///
    /// 接收一个数据报并返回发送方的地址。
    /// 此方法会阻塞直到收到数据或超时。
    /// 如果数据报大小超过缓冲区容量，超出部分会被截断。
    ///
    /// # Arguments
    /// * `buf` - 接收数据的缓冲区，大小决定最大可接收的数据报长度
    ///
    /// # Returns
    /// - `Ok((usize, SocketAddr))` - (实际接收字节数, 发送方地址)
    /// - `Err(Io(...))` - 接收失败（超时、socket 错误等）
    ///
    /// # Warning
    /// UDP 数据报有最大尺寸限制（通常 65507 字节），缓冲区应足够大以避免截断。
    pub fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), TransportError> {
        let (n, addr) = self.socket.recv_from(buf)?;
        Ok((n, addr))
    }
}