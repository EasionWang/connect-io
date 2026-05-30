/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30 00:00:00
 * @FilePath     : /connect-io/src/async_impl/tcp_server.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 15:24:15
 * @Description  : 异步 TCP 服务端多连接管理器
 */
use crate::async_impl::tcp::AsyncTcpTransport;
use crate::TransportError;
use std::net::SocketAddr;
use tokio::net::TcpListener;

/// 异步 TCP 服务端管理器
///
/// 封装 tokio 的 `TcpListener`，提供异步 TCP 服务端的绑定、监听、接受连接等操作。
/// 每次调用 `accept().await` 会异步等待并返回一个新的 `AsyncTcpTransport` 实例。
///
/// # Lifecycle
/// 1. 调用 `bind().await` 异步绑定端口并创建实例
/// 2. 循环调用 `accept().await` 异步接受客户端连接
/// 3. 对每个返回的 `AsyncTcpTransport` 进行异步数据通信
/// 4. 调用 `shutdown().await` 停止监听并释放资源
///
/// # Advantages over Sync Version
/// - 不阻塞线程：单个线程可同时处理多个 accept 操作
/// - 更适合高并发场景：可轻松与 tokio 的 task 并行结合
/// - 与 tokio 生态无缝集成
///
/// # Example
/// ```ignore
/// use connect_io::async_impl::AsyncTcpServerManager;
/// use std::net::SocketAddr;
///
/// let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
/// let server = AsyncTcpServerManager::bind(addr).await?;
///
/// loop {
///     match server.accept().await {
///         Ok(mut client) => {
///             println!("新客户端连接: {:?}", client.peer_addr());
///             // 在新 task 中处理客户端...
///             tokio::spawn(async move {
///                 // 处理通信逻辑
///             });
///         }
///         Err(e) => eprintln!("接受连接失败: {}", e),
///     }
/// }
///
/// server.shutdown().await?;
/// ```
pub struct AsyncTcpServerManager {
    listener: TcpListener,
}

impl AsyncTcpServerManager {
    /// 异步绑定指定地址并启动 TCP 监听
    ///
    /// 创建底层 tokio `TcpListener` 并异步绑定到指定地址。
    /// 绑定成功后即可开始异步接受连接。
    ///
    /// # Arguments
    /// * `addr` - 要绑定的本地 Socket 地址（IP:Port）
    ///
    /// # Returns
    /// - `Ok(AsyncTcpServerManager)` - 异步监听器实例，可开始 accept
    /// - `Err(ConnectionFailed(...))` - 绑定失败（端口被占用、权限不足等）
    ///
    /// # Example
    /// ```ignore
    /// let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    /// let server = AsyncTcpServerManager::bind(addr).await?;
    /// ```
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let listener = TcpListener::bind(&addr).await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        Ok(Self { listener })
    }

    /// 异步接受新的客户端连接
    ///
    /// 异步等待直到有新的客户端连接到达。
    /// 返回的 `AsyncTcpTransport` 实例可直接用于异步数据通信。
    ///
    /// # Returns
    /// - `Ok(AsyncTcpTransport)` - 新连接的异步传输实例
    /// - `Err(ConnectionFailed(...))` - 接受连接失败
    ///
    /// # Non-blocking
    /// 此方法不会阻塞线程，仅挂起当前 task 直到有新连接到达。
    /// 可同时运行多个 accept 操作（如果 listener 被 clone）。
    pub async fn accept(&self) -> Result<AsyncTcpTransport, TransportError> {
        let (stream, _peer_addr) = self.listener.accept().await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;
        Ok(AsyncTcpTransport { stream: Some(stream) })
    }

    /// 获取异步监听器本地绑定的地址
    ///
    /// 返回 TCP 监听器绑定的本地 socket 地址。
    ///
    /// # Returns
    /// - `Some(SocketAddr)` - 本地地址（如 "0.0.0.0:8080"）
    /// - `None` - 获取失败
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.local_addr().ok()
    }

    /// 异步停止监听并释放资源
    ///
    /// 消费 `self`（取得所有权），显式释放底层 `TcpListener` 资源。
    /// 底层 socket 在 drop 时自动关闭，此后不再接受新连接。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`，因为 drop 操作不会产生错误
    ///
    /// # Note
    /// - 调用后此实例被消费，无法再次使用
    /// - 已接受的客户端连接不受影响，它们有独立的生命周期
    /// - 此方法是 async 的以保持接口一致性，但实际操作是同步的
    pub async fn shutdown(self) -> Result<(), TransportError> {
        // tokio TcpListener 在 drop 时自动关闭 socket
        drop(self.listener);
        Ok(())
    }
}
