/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30 00:00:00
 * @FilePath     : /connect-io/src/tcp_server.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:52:27
 * @Description  : 同步 TCP 服务端多连接管理器
 */
use crate::tcp::TcpTransport;
use crate::TransportError;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

/// TCP 服务端管理器（同步）
///
/// 封装 `std::net::TcpListener`，提供 TCP 服务端的绑定、监听、接受连接等操作。
/// 每次调用 `accept()` 会阻塞等待并返回一个新的 `TcpTransport` 实例，
/// 用于与单个客户端通信。
///
/// # Lifecycle
/// 1. 调用 `bind()` 绑定端口并创建实例
/// 2. 循环调用 `accept()` 接受客户端连接
/// 3. 对每个返回的 `TcpTransport` 进行数据通信
/// 4. 调用 `shutdown()` 停止监听并释放资源
///
/// # Thread Safety
/// `TcpServerManager` 本身不是 `Sync` 的，如需多线程 accept，
/// 建议使用 `Arc<TcpListener>` 或切换到异步版本 `AsyncTcpServerManager`。
///
/// # Example
/// ```ignore
/// use connect_io::TcpServerManager;
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
/// let server = TcpServerManager::bind(addr)?;
///
/// loop {
///     match server.accept() {
///         Ok(mut client) => {
///             println!("新客户端连接: {:?}", client.peer_addr());
///             // 处理客户端通信...
///         }
///         Err(e) => eprintln!("接受连接失败: {}", e),
///     }
/// }
///
/// server.shutdown()?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
pub struct TcpServerManager {
    listener: TcpListener,
}

impl TcpServerManager {
    /// 绑定指定地址并启动 TCP 监听
    ///
    /// 创建底层 `TcpListener` 并绑定到指定地址。
    /// 绑定成功后即可开始接受连接。
    ///
    /// # Arguments
    /// * `addr` - 要绑定的本地 Socket 地址（IP:Port）
    ///
    /// # Returns
    /// - `Ok(TcpServerManager)` - 监听器实例，可开始 accept
    /// - `Err(Io(AddrInUse))` - 端口已被占用
    /// - `Err(Io(PermissionDenied))` - 权限不足（绑定 < 1024 端口需要 root）
    ///
    /// # Example
    /// ```ignore
    /// let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    /// let server = TcpServerManager::bind(addr)?;
    /// ```
    pub fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    /// 接受新的客户端连接（阻塞）
    ///
    /// 阻塞等待直到有新的客户端连接到达。
    /// 返回的 `TcpTransport` 实例已设置默认的读/写超时（5秒），
    /// 可直接用于与该客户端通信。
    ///
    /// # Returns
    /// - `Ok(TcpTransport)` - 新连接的传输实例，已配置好超时
    /// - `Err(Io(...))` - 接受连接失败或设置超时失败
    ///
    /// # Blocking
    /// 此方法会阻塞当前线程直到有新连接到达。
    /// 如需非阻塞或多客户端处理，请使用异步版本 `AsyncTcpServerManager`。
    pub fn accept(&self) -> Result<TcpTransport, TransportError> {
        let (stream, _peer_addr) = self.listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(TcpTransport::from_stream(stream))
    }

    /// 获取监听器本地绑定的地址
    ///
    /// 返回 TCP 监听器绑定的本地 socket 地址。
    /// 如果绑定时端口指定为 0，此处可获取操作系统实际分配的端口号。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地绑定地址（如 "0.0.0.0:8080"）
    /// - `None` - 获取失败
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.local_addr().ok()
    }

    /// 停止监听并释放资源
    ///
    /// 消费 `self`（取得所有权），显式释放底层 `TcpListener` 资源。
    /// 底层 socket 在 drop 时自动关闭，此后不再接受新连接。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`，因为 drop 操作不会产生错误
    ///
    /// # Note
    /// 调用后此 `TcpServerManager` 实例被消费，无法再次使用。
    /// 已接受的客户端连接不受影响，它们有自己的独立生命周期。
    pub fn shutdown(self) -> Result<(), TransportError> {
        // TcpListener 在 drop 时自动关闭 socket
        // 显式 drop listener 以确保资源立即释放
        drop(self.listener);
        Ok(())
    }
}
