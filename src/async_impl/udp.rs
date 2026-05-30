/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:54
 * @FilePath     : /connect-io/src/async_impl/udp.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:21:18
 * @Description  : 异步 UDP 传输实现
 */
use crate::async_impl::{AsyncTransport, AsyncTransportConfig, ConnectionState};
use crate::TransportError;
use async_trait::async_trait;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UdpSocket;

/// 异步 UDP 传输实现
///
/// 封装 tokio 的 `UdpSocket`，提供基于 UDP 协议的异步数据报传输。
/// 支持有连接模式（connect 到对端后使用 AsyncRead/AsyncWrite）
/// 和无连接模式（使用 send_to/recv_from 指定目标地址）。
///
/// # Characteristics
/// - **协议**: UDP（用户数据报协议），无连接、不可靠、无序
/// - **运行时**: 基于 tokio，不阻塞线程
/// - **模式**: 支持已连接和无连接两种使用方式
/// - **数据边界**: 保持消息边界，每次 recv 对应一次 send
///
/// # Usage Modes
/// 1. **已连接模式**: 配置时指定 peer_addr，之后可用 AsyncRead/AsyncWrite
/// 2. **无连接模式**: 不指定 peer_addr，必须用 send_to/recv_from
///
/// # Example (Connected Mode)
/// ```ignore
/// use connect_io::async_impl::{AsyncUdpTransport, AsyncTransport, AsyncTransportConfig};
/// use std::net::SocketAddr;
///
/// let bind: SocketAddr = "0.0.0.0:9000".parse().unwrap();
/// let peer: SocketAddr = "192.168.1.100:9000".parse().unwrap();
/// let config = AsyncTransportConfig::Udp { bind_addr: bind, peer_addr: Some(peer) };
/// let mut transport = AsyncUdpTransport::connect(config).await?;
///
/// use tokio::io::AsyncWriteExt;
/// transport.write_all(b"Hello Async UDP").await?;
/// ```
pub struct AsyncUdpTransport {
    socket: UdpSocket,
    connected: bool,
}

#[async_trait]
impl AsyncTransport for AsyncUdpTransport {
    /// 异步建立 UDP socket
    ///
    /// 绑定本地地址并可选地异步连接到对端。
    /// 如果指定了 peer_addr，socket 会进入已连接模式。
    ///
    /// # Arguments
    /// * `config` - 必须为 `Udp` 变体
    ///
    /// # Returns
    /// 成功返回已初始化的 `AsyncUdpTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected UDP config")` - 配置类型不是 Udp
    /// - `ConnectionFailed(...)` - 绑定或连接失败
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError> {
        if let AsyncTransportConfig::Udp { bind_addr, peer_addr } = config {
            let socket = UdpSocket::bind(bind_addr).await
                .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

            let connected = if let Some(peer) = peer_addr {
                socket.connect(peer).await
                    .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
                true
            } else {
                false
            };

            Ok(Self { socket, connected })
        } else {
            Err(TransportError::Config("Expected UDP config".into()))
        }
    }

    /// 异步关闭 UDP socket
    ///
    /// UDP 无连接概念，此方法仅做兼容性处理，始终返回成功。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    async fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    /// 检查异步 UDP 是否处于已连接模式
    ///
    /// 返回 socket 是否已通过 connect() 绑定到对端地址。
    ///
    /// # Returns
    /// - `true` - 已连接模式（可使用 AsyncRead/AsyncWrite）
    /// - `false` - 无连接模式（需使用 send_to/recv_from）
    fn is_connected(&self) -> bool {
        self.connected
    }

    /// 设置超时时间（占位实现）
    ///
    /// tokio 异步 I/O 不使用传统超时机制。
    /// 此方法为空操作，始终返回成功。
    fn set_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), TransportError> {
        Ok(())
    }

    /// 获取异步 UDP 本地绑定地址
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址
    /// - `None` - 获取失败
    fn local_addr(&self) -> Option<SocketAddr> {
        self.socket.local_addr().ok()
    }

    /// 获取异步 UDP 远程对端地址
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 对端地址（仅已连接模式）
    /// - `None` - 无连接模式或获取失败
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.socket.peer_addr().ok()
    }

    /// 获取异步 UDP 连接状态
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

/// AsyncRead 实现 - 异步读取 UDP 数据（已连接模式）
///
/// 仅在已连接模式下可用，委托给 `UdpSocket::poll_recv`。
/// 如果未连接，返回 `NotConnected` 错误。
impl AsyncRead for AsyncUdpTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.connected {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            )));
        }

        let socket = &this.socket;
        match socket.poll_recv(cx, buf) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// AsyncWrite 实现 - 异步写入 UDP 数据（已连接模式）
///
/// 仅在已连接模式下可用，委托给 `UdpSocket::poll_send`。
/// 如果未连接，返回 `NotConnected` 错误。
impl AsyncWrite for AsyncUdpTransport {
    /// 异步向 UDP socket 写入数据
    ///
    /// # Arguments
    /// * `cx` - 异步上下文
    /// * `buf` - 待写入的数据
    ///
    /// # Returns
    /// - `Poll::Ready(Ok(n))` - 成功发送 n 字节
    /// - `Poll::Ready(Err(NotConnected))` - 未连接
    /// - `Poll::Pending` - 发送缓冲区满
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();
        if !this.connected {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "UDP socket not connected",
            )));
        }

        let socket = &this.socket;
        match socket.poll_send(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => std::task::Poll::Ready(Ok(n)),
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncUdpTransport {
    /// 无连接模式下异步发送数据到指定地址
    ///
    /// 将数据报异步发送到指定的目标地址，无需预先 connect。
    /// 此方法在已连接和无连接模式下均可使用。
    ///
    /// # Arguments
    /// * `buf` - 待发送的数据缓冲区
    /// * `addr` - 目标 Socket 地址（IP:Port）
    ///
    /// # Returns
    /// - `Ok(usize)` - 实际发送的字节数
    /// - `Err(ConnectionFailed(...))` - 发送失败
    ///
    /// # Note
    /// UDP 不保证可靠传输，发送成功不代表对端收到数据。
    pub async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> Result<usize, TransportError> {
        let n = self.socket.send_to(buf, addr).await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        Ok(n)
    }

    /// 无连接模式下异步从任意地址接收数据
    ///
    /// 异步接收一个数据报并返回发送方的地址。
    /// 此方法会等待直到收到数据或超时。
    ///
    /// # Arguments
    /// * `buf` - 接收数据的缓冲区，大小决定最大可接收的数据报长度
    ///
    /// # Returns
    /// - `Ok((usize, SocketAddr))` - (实际接收字节数, 发送方地址)
    /// - `Err(ConnectionFailed(...))` - 接收失败（超时、socket 错误等）
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr), TransportError> {
        let (n, addr) = self.socket.recv_from(buf).await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        Ok((n, addr))
    }

    // ============================================================
    // 广播
    // ============================================================

    /// 设置 UDP socket 的广播模式
    ///
    /// 启用后，socket 可以向广播地址（如 255.255.255.255）发送数据报。
    /// 默认情况下广播是关闭的。
    ///
    /// # Arguments
    /// * `flag` - true 启用广播，false 禁用
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败
    pub fn set_broadcast(&self, flag: bool) -> Result<(), TransportError> {
        self.socket.set_broadcast(flag)?;
        Ok(())
    }

    // ============================================================
    // 组播 (IPv4)
    // ============================================================

    /// 加入 IPv4 组播组
    ///
    /// 将 socket 加入指定的 IPv4 组播地址，之后可以接收发往该组播地址的数据报。
    ///
    /// # Arguments
    /// * `multiaddr` - 要加入的组播地址（如 224.0.0.1）
    /// * `iface` - 加入组播组使用的本地接口地址（如 0.0.0.0 表示任意接口）
    ///
    /// # Returns
    /// - `Ok(())` - 加入成功
    /// - `Err(Io(...))` - 加入失败
    pub fn join_multicast_v4(
        &self,
        multiaddr: Ipv4Addr,
        iface: Ipv4Addr,
    ) -> Result<(), TransportError> {
        self.socket.join_multicast_v4(multiaddr, iface)?;
        Ok(())
    }

    /// 离开 IPv4 组播组
    ///
    /// # Arguments
    /// * `multiaddr` - 要离开的组播地址
    /// * `iface` - 离开组播组使用的本地接口地址
    ///
    /// # Returns
    /// - `Ok(())` - 离开成功
    /// - `Err(Io(...))` - 离开失败
    pub fn leave_multicast_v4(
        &self,
        multiaddr: Ipv4Addr,
        iface: Ipv4Addr,
    ) -> Result<(), TransportError> {
        self.socket.leave_multicast_v4(multiaddr, iface)?;
        Ok(())
    }

    /// 设置 IPv4 组播数据是否回环到本地 socket
    ///
    /// # Arguments
    /// * `flag` - true 启用回环，false 禁用
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败
    pub fn set_multicast_loop_v4(&self, flag: bool) -> Result<(), TransportError> {
        self.socket.set_multicast_loop_v4(flag)?;
        Ok(())
    }

    /// 设置 IPv4 组播数据报的 TTL（生存时间）
    ///
    /// # Arguments
    /// * `ttl` - TTL 值（1-255），1 表示仅本地网络
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败
    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> Result<(), TransportError> {
        self.socket.set_multicast_ttl_v4(ttl)?;
        Ok(())
    }

    // ============================================================
    // 组播 (IPv6)
    // ============================================================

    /// 加入 IPv6 组播组
    ///
    /// # Arguments
    /// * `multiaddr` - 要加入的 IPv6 组播地址（如 ff02::1）
    /// * `iface` - 接口索引（0 表示默认接口）
    ///
    /// # Returns
    /// - `Ok(())` - 加入成功
    /// - `Err(Io(...))` - 加入失败
    pub fn join_multicast_v6(
        &self,
        multiaddr: Ipv6Addr,
        iface: u32,
    ) -> Result<(), TransportError> {
        self.socket.join_multicast_v6(&multiaddr, iface)?;
        Ok(())
    }

    /// 离开 IPv6 组播组
    ///
    /// # Arguments
    /// * `multiaddr` - 要离开的 IPv6 组播地址
    /// * `iface` - 接口索引
    ///
    /// # Returns
    /// - `Ok(())` - 离开成功
    /// - `Err(Io(...))` - 离开失败
    pub fn leave_multicast_v6(
        &self,
        multiaddr: Ipv6Addr,
        iface: u32,
    ) -> Result<(), TransportError> {
        self.socket.leave_multicast_v6(&multiaddr, iface)?;
        Ok(())
    }

    /// 设置 IPv6 组播数据是否回环到本地 socket
    ///
    /// # Arguments
    /// * `flag` - true 启用回环，false 禁用
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Io(...))` - 系统调用失败
    pub fn set_multicast_loop_v6(&self, flag: bool) -> Result<(), TransportError> {
        self.socket.set_multicast_loop_v6(flag)?;
        Ok(())
    }
}
