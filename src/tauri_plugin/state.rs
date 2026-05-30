/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30
 * @FilePath     : /connect-io/src/tauri_plugin/state.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30
 * @Description  : Tauri 集成层状态管理，管理多个活跃传输会话
 */
#![allow(non_snake_case)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use crate::async_impl::AsyncTransport;
use crate::async_impl::AsyncTransportConfig;
use crate::ConnectionState;
use tauri::Emitter;
use tokio::sync::mpsc;

/// 会话命令：从 Tauri command 发送到后台读写任务
#[derive(Debug)]
pub enum SessionCommand {
    /// 写入数据到连接
    Write { data: Vec<u8> },
    /// UDP 无连接模式发送数据到指定地址
    SendTo { data: Vec<u8>, addr: SocketAddr },
    /// 设置 UDP 广播模式
    #[cfg(feature = "udp")]
    SetBroadcast { flag: bool },
    /// 加入 IPv4 组播组
    #[cfg(feature = "udp")]
    JoinMulticastV4 { multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr },
    /// 离开 IPv4 组播组
    #[cfg(feature = "udp")]
    LeaveMulticastV4 { multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr },
    /// 加入 IPv6 组播组
    #[cfg(feature = "udp")]
    JoinMulticastV6 { multiaddr: std::net::Ipv6Addr, iface: u32 },
    /// 离开 IPv6 组播组
    #[cfg(feature = "udp")]
    LeaveMulticastV6 { multiaddr: std::net::Ipv6Addr, iface: u32 },
    /// 关闭会话
    Close,
}

/// 会话事件：从后台任务回传给 Tauri 层
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// 收到数据
    DataReceived { data: Vec<u8> },
    /// 发生错误
    Error { message: String },
    /// 连接已关闭
    Closed,
    /// 连接状态变更
    StateChanged { state: ConnectionState },
}

/// 单个异步传输会话的内部状态
struct AsyncSession {
    /// 会话唯一标识
    id: String,
    /// 传输配置（用于展示和重建）
    config: AsyncTransportConfig,
    /// 命令发送端，用于向后台任务发送写入/关闭指令
    tx: mpsc::Sender<SessionCommand>,
    /// 事件接收端（Arc<Mutex<>> 包裹以支持跨线程非阻塞读取）
    rx: Arc<Mutex<mpsc::Receiver<SessionEvent>>>,
}

impl AsyncSession {
    /// 获取协议类型描述字符串
    fn protocol_name(&self) -> &'static str {
        match &self.config {
            AsyncTransportConfig::TcpClient { .. } => "tcp",
            AsyncTransportConfig::TcpServer { .. } => "tcp",
            AsyncTransportConfig::Udp { .. } => "udp",
            #[cfg(feature = "serial")]
            AsyncTransportConfig::Serial { .. } => "serial",
        }
    }

    /// 获取角色类型描述字符串
    fn role_name(&self) -> &'static str {
        match &self.config {
            AsyncTransportConfig::TcpClient { .. } => "client",
            AsyncTransportConfig::TcpServer { .. } => "server",
            AsyncTransportConfig::Udp { .. } => "client",
            #[cfg(feature = "serial")]
            AsyncTransportConfig::Serial { .. } => "client",
        }
    }
}

/// 传输会话全局状态管理器
///
/// 管理所有活跃的异步传输会话实例。
/// 每个会话通过 `tokio::spawn` 启动独立的后台读写任务，
/// 通过 mpsc channel 与主线程通信。
///
/// # 线程安全
///
/// 内部使用 `Arc<Mutex<HashMap>>` 保护 sessions 映射。
/// **重要约束**：持有锁期间禁止调用 Tauri 事件发送，
/// 必须先 drop 锁再进行任何 IPC 操作。
pub struct TransportState {
    sessions: Arc<Mutex<HashMap<String, AsyncSession>>>,
}

impl Default for TransportState {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportState {
    /// 创建新的传输状态管理器
    ///
    /// # 返回
    ///
    /// 初始化后的 `TransportState` 实例，sessions 集合为空
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 创建并启动一个新的传输会话
    ///
    /// 根据配置创建异步传输实例，并通过 `tokio::spawn` 启动后台读写任务。
    /// 后台任务通过 mpsc channel 接收命令和回传事件。
    ///
    /// # 参数
    ///
    /// - `session_id`: 会话唯一标识符
    /// - `config`: 异步传输配置（TCP/UDP/Serial）
    /// - `app_handle`: Tauri 应用句柄，用于向前端推送事件
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 会话创建成功并已在后台运行
    /// - `Err(String)`: 配置无效、连接失败或 session_id 已存在
    pub fn create_session(
        &self,
        session_id: String,
        config: AsyncTransportConfig,
        app_handle: tauri::AppHandle,
    ) -> Result<(), String> {
        // 检查是否已存在同名 session（短时间持锁）
        {
            let sessions = self
                .sessions
                .lock()
                .map_err(|e| format!("Failed to lock sessions: {}", e))?;
            if sessions.contains_key(&session_id) {
                return Err(format!("Session '{}' already exists", session_id));
            }
        }
        // 锁已释放

        let (tx_cmd, rx_cmd) = mpsc::channel::<SessionCommand>(32);
        let (tx_event, rx_event) = mpsc::channel::<SessionEvent>(64);

        let sid = session_id.clone();
        let cfg = config.clone();

        tokio::spawn(async move {
            FunEvent_run_session_task(sid, cfg, rx_cmd, tx_event, app_handle).await;
        });

        // 注册 session 到管理器（重新获取锁）
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        sessions.insert(
            session_id.clone(),
            AsyncSession {
                id: session_id,
                config,
                tx: tx_cmd,
                rx: Arc::new(Mutex::new(rx_event)),
            },
        );

        Ok(())
    }

    /// 向指定会话写入数据
    ///
    /// 数据通过 mpsc channel 发送给后台任务执行实际 I/O。
    ///
    /// # 参数
    ///
    /// - `session_id`: 目标会话 ID
    /// - `data`: 待写入的二进制数据
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 写入命令已发送
    /// - `Err(String)`: session 不存在或 channel 已关闭
    pub fn write(&self, session_id: &str, data: Vec<u8>) -> Result<(), String> {
        let tx = self.get_session_tx(session_id)?;
        tx.blocking_send(SessionCommand::Write { data })
            .map_err(|e| format!("Failed to send write command: {}", e))
    }

    /// UDP 无连接模式：向指定地址发送数据
    ///
    /// # 参数
    ///
    /// - `session_id`: UDP 会话 ID
    /// - `data`: 待发送的数据
    /// - `addr`: 目标 Socket 地址
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 发送命令已入队
    /// - `Err(String)`: session 不存在或 channel 已关闭
    pub fn send_to(
        &self,
        session_id: &str,
        data: Vec<u8>,
        addr: SocketAddr,
    ) -> Result<(), String> {
        let tx = self.get_session_tx(session_id)?;
        tx.blocking_send(SessionCommand::SendTo { data, addr })
            .map_err(|e| format!("Failed to send send_to command: {}", e))
    }

    /// 从指定会话的非阻塞读取可用数据
    ///
    /// 尝试从 event channel 接收已缓存的数据事件。
    /// 此方法不会阻塞，无数据时返回空列表。
    ///
    /// # 参数
    ///
    /// - `session_id`: 目标会话 ID
    ///
    /// # 返回
    ///
    /// - `Ok(Vec<Vec<u8>>)`: 收到的所有数据块列表（按时间顺序）
    /// - `Err(String)`: session 不存在
    pub fn read(&self, session_id: &str) -> Result<Vec<Vec<u8>>, String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        // 获取 rx 的 Arc 克隆后立即释放 sessions 锁
        let rx_arc = session.rx.clone();
        // 锁随作用域结束自动释放

        let mut rx = rx_arc.lock()
            .map_err(|e| format!("Failed to lock event receiver: {}", e))?;

        let mut result = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(SessionEvent::DataReceived { data }) => {
                    result.push(data);
                }
                Ok(SessionEvent::StateChanged { state }) => {
                    log::debug!("Session {} state changed to {:?}", session_id, state);
                }
                Ok(SessionEvent::Error { message }) => {
                    log::warn!("Session {} error: {}", session_id, message);
                }
                Ok(SessionEvent::Closed) => {
                    log::info!("Session {} closed by backend task", session_id);
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    log::warn!("Session {} event channel disconnected", session_id);
                    break;
                }
            }
        }
        // 锁随作用域结束自动释放

        Ok(result)
    }

    /// 关闭指定会话
    ///
    /// 向后台任务发送 Close 命令，并从 sessions 映射中移除该会话。
    ///
    /// # 参数
    ///
    /// - `session_id`: 要关闭的会话 ID
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 关闭命令已发送且会话已移除
    /// - `Err(String)`: session 不存在
    pub fn close(&self, session_id: &str) -> Result<(), String> {
        let tx = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|e| format!("Failed to lock sessions: {}", e))?;

            let session = sessions
                .remove(session_id)
                .ok_or_else(|| format!("Session '{}' not found", session_id))?;
            session.tx
        };
        // 锁已释放

        if let Err(e) = tx.blocking_send(SessionCommand::Close) {
            log::warn!(
                "Failed to send close command for session '{}': {}",
                session_id,
                e
            );
        }

        Ok(())
    }

    /// 获取指定会话的当前连接状态
    ///
    /// 通过检查命令 channel 是否存活来判断连接是否仍然活跃。
    ///
    /// # 参数
    ///
    /// - `session_id`: 目标会话 ID
    ///
    /// # 返回
    ///
    /// - `Ok(ConnectionState)`: 当前连接状态
    /// - `Err(String)`: session 不存在
    pub fn get_state(&self, session_id: &str) -> Result<ConnectionState, String> {
        let tx = self.get_session_tx(session_id)?;
        if tx.is_closed() {
            Ok(ConnectionState::Disconnected)
        } else {
            Ok(ConnectionState::Connected)
        }
    }

    /// 列出所有活跃会话的基本信息
    ///
    /// # 返回
    ///
    /// - `Ok(Vec<(String, String, String)>)`: (session_id, protocol, role) 列表
    pub fn list_sessions(&self) -> Result<Vec<(String, String, String)>, String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let result = sessions
            .values()
            .map(|s| (s.id.clone(), s.protocol_name().to_string(), s.role_name().to_string()))
            .collect();
        // 锁随作用域结束自动释放

        Ok(result)
    }

    /// 关闭所有活跃会话并清空 sessions 映射
    ///
    /// 逐个发送 Close 命令后清空映射。即使个别关闭失败也继续处理其余会话。
    pub fn cleanup(&self) {
        let all_sessions: HashMap<String, mpsc::Sender<SessionCommand>> = {
            let mut sessions = match self.sessions.lock() {
                Ok(s) => s,
                Err(e) => {
                    log::error!("Failed to lock sessions for cleanup: {}", e);
                    return;
                }
            };
            sessions.drain().map(|(id, s)| (id, s.tx)).collect()
        };
        // 锁已释放

        for (session_id, tx) in all_sessions {
            if let Err(e) = tx.blocking_send(SessionCommand::Close) {
                log::warn!(
                    "Failed to send close command for session '{}': {}",
                    session_id,
                    e
                );
            }
        }
    }

    // ============================================================
    // UDP 广播 / 组播
    // ============================================================

    /// 设置指定 UDP 会话的广播模式
    ///
    /// # 参数
    /// - `session_id`: UDP 会话 ID
    /// - `flag`: true 启用广播
    ///
    /// # 返回
    /// - `Ok(())` - 设置成功
    /// - `Err(String)` - session 不存在或非 UDP 会话
    #[cfg(feature = "udp")]
    pub fn set_broadcast(&self, session_id: &str, flag: bool) -> Result<(), String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        match &session.config {
            AsyncTransportConfig::Udp { .. } => {}
            _ => return Err("set_broadcast is only available for UDP sessions".to_string()),
        }

        let tx = session.tx.clone();
        drop(sessions);

        tx.blocking_send(SessionCommand::SetBroadcast { flag })
            .map_err(|e| format!("Failed to send set_broadcast command: {}", e))
    }

    /// 加入 IPv4 组播组
    #[cfg(feature = "udp")]
    pub fn join_multicast_v4(&self, session_id: &str, multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr) -> Result<(), String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        match &session.config {
            AsyncTransportConfig::Udp { .. } => {}
            _ => return Err("join_multicast_v4 is only available for UDP sessions".to_string()),
        }

        let tx = session.tx.clone();
        drop(sessions);

        tx.blocking_send(SessionCommand::JoinMulticastV4 { multiaddr, iface })
            .map_err(|e| format!("Failed to send join_multicast_v4 command: {}", e))
    }

    /// 离开 IPv4 组播组
    #[cfg(feature = "udp")]
    pub fn leave_multicast_v4(&self, session_id: &str, multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr) -> Result<(), String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        match &session.config {
            AsyncTransportConfig::Udp { .. } => {}
            _ => return Err("leave_multicast_v4 is only available for UDP sessions".to_string()),
        }

        let tx = session.tx.clone();
        drop(sessions);

        tx.blocking_send(SessionCommand::LeaveMulticastV4 { multiaddr, iface })
            .map_err(|e| format!("Failed to send leave_multicast_v4 command: {}", e))
    }

    /// 加入 IPv6 组播组
    #[cfg(feature = "udp")]
    pub fn join_multicast_v6(&self, session_id: &str, multiaddr: std::net::Ipv6Addr, iface: u32) -> Result<(), String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        match &session.config {
            AsyncTransportConfig::Udp { .. } => {}
            _ => return Err("join_multicast_v6 is only available for UDP sessions".to_string()),
        }

        let tx = session.tx.clone();
        drop(sessions);

        tx.blocking_send(SessionCommand::JoinMulticastV6 { multiaddr, iface })
            .map_err(|e| format!("Failed to send join_multicast_v6 command: {}", e))
    }

    /// 离开 IPv6 组播组
    #[cfg(feature = "udp")]
    pub fn leave_multicast_v6(&self, session_id: &str, multiaddr: std::net::Ipv6Addr, iface: u32) -> Result<(), String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        let session = sessions.get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        match &session.config {
            AsyncTransportConfig::Udp { .. } => {}
            _ => return Err("leave_multicast_v6 is only available for UDP sessions".to_string()),
        }

        let tx = session.tx.clone();
        drop(sessions);

        tx.blocking_send(SessionCommand::LeaveMulticastV6 { multiaddr, iface })
            .map_err(|e| format!("Failed to send leave_multicast_v6 command: {}", e))
    }

    // ============================================================
    // 内部辅助方法
    // ============================================================

    /// 获取指定 session 的命令发送端克隆（短时间持锁）
    fn get_session_tx(&self, session_id: &str) -> Result<mpsc::Sender<SessionCommand>, String> {
        let sessions = self
            .sessions
            .lock()
            .map_err(|e| format!("Failed to lock sessions: {}", e))?;

        sessions
            .get(session_id)
            .map(|s| s.tx.clone())
            .ok_or_else(|| format!("Session '{}' not found", session_id))
    }
}

// ============================================================
// 后台会话任务 —— 核心读写循环
// ============================================================

/// 具体传输实例的枚举包装，保留类型信息以支持协议特有操作
enum SessionTransport {
    /// TCP 传输（客户端或服务端 accept 得到的连接）
    Tcp(crate::async_impl::tcp::AsyncTcpTransport),
    /// UDP 传输
    Udp(crate::async_impl::udp::AsyncUdpTransport),
    /// 串口传输
    #[cfg(feature = "serial")]
    Serial(crate::async_impl::serial::AsyncSerialTransport),
}

impl SessionTransport {
    /// 异步写入数据
    async fn write_all(&mut self, data: &[u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncWriteExt;
        match self {
            SessionTransport::Tcp(t) => t.write_all(data).await.map(|_| data.len()),
            SessionTransport::Udp(u) => u.write_all(data).await.map(|_| data.len()),
            #[cfg(feature = "serial")]
            SessionTransport::Serial(s) => s.write_all(data).await.map(|_| data.len()),
        }
    }

    /// 异步读取数据到缓冲区
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        match self {
            SessionTransport::Tcp(t) => t.read(buf).await,
            SessionTransport::Udp(u) => u.read(buf).await,
            #[cfg(feature = "serial")]
            SessionTransport::Serial(s) => s.read(buf).await,
        }
    }

    /// UDP 无连接发送（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    async fn send_to(&mut self, data: &[u8], addr: SocketAddr) -> Result<usize, crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.send_to(data, addr).await,
            _ => Err(crate::TransportError::Config("send_to is only available for UDP".into())),
        }
    }

    /// 设置广播模式（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    fn set_broadcast(&self, flag: bool) -> Result<(), crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.set_broadcast(flag),
            _ => Err(crate::TransportError::Config("set_broadcast is only available for UDP".into())),
        }
    }

    /// 加入 IPv4 组播组（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    fn join_multicast_v4(&self, multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr) -> Result<(), crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.join_multicast_v4(multiaddr, iface),
            _ => Err(crate::TransportError::Config("join_multicast_v4 is only available for UDP".into())),
        }
    }

    /// 离开 IPv4 组播组（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    fn leave_multicast_v4(&self, multiaddr: std::net::Ipv4Addr, iface: std::net::Ipv4Addr) -> Result<(), crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.leave_multicast_v4(multiaddr, iface),
            _ => Err(crate::TransportError::Config("leave_multicast_v4 is only available for UDP".into())),
        }
    }

    /// 加入 IPv6 组播组（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    fn join_multicast_v6(&self, multiaddr: std::net::Ipv6Addr, iface: u32) -> Result<(), crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.join_multicast_v6(multiaddr, iface),
            _ => Err(crate::TransportError::Config("join_multicast_v6 is only available for UDP".into())),
        }
    }

    /// 离开 IPv6 组播组（仅对 Udp 变体有效）
    #[cfg(feature = "udp")]
    fn leave_multicast_v6(&self, multiaddr: std::net::Ipv6Addr, iface: u32) -> Result<(), crate::TransportError> {
        match self {
            SessionTransport::Udp(u) => u.leave_multicast_v6(multiaddr, iface),
            _ => Err(crate::TransportError::Config("leave_multicast_v6 is only available for UDP".into())),
        }
    }

    /// 关闭传输
    async fn shutdown(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            SessionTransport::Tcp(t) => t.shutdown().await,
            SessionTransport::Udp(u) => u.shutdown().await,
            #[cfg(feature = "serial")]
            SessionTransport::Serial(s) => s.shutdown().await,
        }
    }

    /// 获取本地地址
    #[allow(dead_code)]
    fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            SessionTransport::Tcp(t) => t.local_addr(),
            SessionTransport::Udp(u) => u.local_addr(),
            #[cfg(feature = "serial")]
            SessionTransport::Serial(_) => None,
        }
    }

    /// 获取对端地址
    #[allow(dead_code)]
    fn peer_addr(&self) -> Option<SocketAddr> {
        match self {
            SessionTransport::Tcp(t) => t.peer_addr(),
            SessionTransport::Udp(u) => u.peer_addr(),
            #[cfg(feature = "serial")]
            SessionTransport::Serial(_) => None,
        }
    }
}

/// 根据配置创建具体的 SessionTransport 实例
///
/// # 参数
///
/// - `config`: 异步传输配置
///
/// # 返回
///
/// - `Ok(SessionTransport)`: 包装后的具体传输实例
/// - `Err(TransportError)`: 创建失败
async fn FunEvent_create_session_transport(
    config: AsyncTransportConfig,
) -> Result<SessionTransport, crate::TransportError> {
    use crate::async_impl::{
        AsyncSerialTransport, AsyncTcpTransport, AsyncUdpTransport,
    };

    match config {
        AsyncTransportConfig::TcpClient { addr } => {
            let tcp = AsyncTcpTransport::connect(AsyncTransportConfig::TcpClient { addr }).await?;
            Ok(SessionTransport::Tcp(tcp))
        }
        AsyncTransportConfig::TcpServer { bind_addr } => {
            let tcp =
                AsyncTcpTransport::connect(AsyncTransportConfig::TcpServer { bind_addr }).await?;
            Ok(SessionTransport::Tcp(tcp))
        }
        AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr,
        } => {
            let udp = AsyncUdpTransport::connect(AsyncTransportConfig::Udp {
                bind_addr,
                peer_addr,
            })
            .await?;
            Ok(SessionTransport::Udp(udp))
        }
        #[cfg(feature = "serial")]
        AsyncTransportConfig::Serial {
            port,
            baud_rate,
            data_bits,
            stop_bits,
            parity,
            flow_control,
        } => {
            let serial = AsyncSerialTransport::connect(AsyncTransportConfig::Serial {
                port,
                baud_rate,
                data_bits,
                stop_bits,
                parity,
                flow_control,
            })
            .await?;
            Ok(SessionTransport::Serial(serial))
        }
    }
}

/// 运行单个会话的后台读写任务
///
/// 这是每个 session 的核心事件循环：
/// 1. 根据 config 创建具体类型的传输实例（保留类型信息）
/// 2. 使用 `tokio::select!` 同时监听命令通道和数据读取
/// 3. 收到 Close 或 I/O 错误时退出并发送 Closed 事件
///
/// **性能约束**：
/// - 读取间隔使用 5ms sleep 避免忙等待（满足 ≤5ms 要求）
/// - 持有锁时禁止 Tauri 事件发送（本函数内部无跨线程锁）
/// - 缓冲区预分配 8192 字节，避免频繁扩容
///
/// # 参数
///
/// - `session_id`: 会话标识
/// - `config`: 传输配置
/// - `rx_cmd`: 命令接收端
/// - `tx_event`: 事件发送端
/// - `app_handle`: Tauri 应用句柄
async fn FunEvent_run_session_task(
    session_id: String,
    config: AsyncTransportConfig,
    mut rx_cmd: mpsc::Receiver<SessionCommand>,
    tx_event: mpsc::Sender<SessionEvent>,
    app_handle: tauri::AppHandle,
) {
    log::info!(
        "[{}] Starting session task, config: {:?}",
        session_id,
        config_type_str(&config)
    );

    // 创建具体类型的传输实例
    let mut transport = match FunEvent_create_session_transport(config.clone()).await {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("Failed to create transport: {}", e);
            log::error!("[{}] {}", session_id, msg);
            let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
            let _ = tx_event.send(SessionEvent::Closed).await;
            return;
        }
    };

    // 发送初始状态变更事件
    let _ = tx_event
        .send(SessionEvent::StateChanged {
            state: ConnectionState::Connected,
        })
        .await;

    // 推送 Tauri 事件通知前端连接建立
    let sid_for_event = session_id.clone();
    let _ = app_handle.emit(
        &format!("transport://{}", session_id),
        serde_json::json!({
            "type": "state_changed",
            "session_id": sid_for_event,
            "state": "Connected",
        }),
    );

    // 预分配读取缓冲区（8KB，避免热路径频繁扩容）
    let mut read_buf = vec![0u8; 8192];
    let mut sleep_interval = tokio::time::interval(std::time::Duration::from_millis(5));

    // 主事件循环
    loop {
        tokio::select! {
            // 分支1：处理来自前端的命令
            cmd = rx_cmd.recv() => {
                let should_exit = FunEvent_handle_command(
                    &mut transport,
                    cmd,
                    &tx_event,
                    &session_id,
                ).await;

                if should_exit {
                    break;
                }
            }

            // 分支2：定时读取数据（5ms 间隔，满足休眠约束）
            _ = sleep_interval.tick() => {
                let should_exit = FunEvent_do_read(
                    &mut transport,
                    &mut read_buf,
                    &tx_event,
                    &app_handle,
                    &session_id,
                ).await;

                if should_exit {
                    break;
                }
            }
        }
    }

    // 清理资源
    if let Err(e) = transport.shutdown().await {
        log::warn!("[{}] Transport shutdown error: {}", session_id, e);
    }

    log::info!("[{}] Session task ended", session_id);

    // 发送关闭事件
    let _ = tx_event.send(SessionEvent::Closed).await;
}

/// 处理从命令通道接收到的指令
///
/// # 返回值
///
/// - `true`: 应退出主循环
/// - `false`: 继续循环
async fn FunEvent_handle_command(
    transport: &mut SessionTransport,
    cmd: Option<SessionCommand>,
    tx_event: &mpsc::Sender<SessionEvent>,
    session_id: &str,
) -> bool {
    match cmd {
        Some(SessionCommand::Write { data }) => {
            match transport.write_all(&data).await {
                Ok(n) => {
                    log::trace!("[{}] wrote {} bytes", session_id, n);
                }
                Err(e) => {
                    let msg = format!("Write error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            false
        }
        Some(SessionCommand::SendTo { data, addr }) => {
            #[cfg(feature = "udp")]
            match transport.send_to(&data, addr).await {
                Ok(n) => {
                    log::trace!("[{}] sent {} bytes to {}", session_id, n, addr);
                }
                Err(e) => {
                    let msg = format!("SendTo error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = (data, addr);
            false
        }
        Some(SessionCommand::SetBroadcast { flag }) => {
            #[cfg(feature = "udp")]
            match transport.set_broadcast(flag) {
                Ok(()) => {
                    log::trace!("[{}] set_broadcast({}) ok", session_id, flag);
                }
                Err(e) => {
                    let msg = format!("SetBroadcast error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = flag;
            false
        }
        Some(SessionCommand::JoinMulticastV4 { multiaddr, iface }) => {
            #[cfg(feature = "udp")]
            match transport.join_multicast_v4(multiaddr, iface) {
                Ok(()) => {
                    log::trace!("[{}] join_multicast_v4({}) ok", session_id, multiaddr);
                }
                Err(e) => {
                    let msg = format!("JoinMulticastV4 error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = (multiaddr, iface);
            false
        }
        Some(SessionCommand::LeaveMulticastV4 { multiaddr, iface }) => {
            #[cfg(feature = "udp")]
            match transport.leave_multicast_v4(multiaddr, iface) {
                Ok(()) => {
                    log::trace!("[{}] leave_multicast_v4({}) ok", session_id, multiaddr);
                }
                Err(e) => {
                    let msg = format!("LeaveMulticastV4 error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = (multiaddr, iface);
            false
        }
        Some(SessionCommand::JoinMulticastV6 { multiaddr, iface }) => {
            #[cfg(feature = "udp")]
            match transport.join_multicast_v6(multiaddr, iface) {
                Ok(()) => {
                    log::trace!("[{}] join_multicast_v6({}) ok", session_id, multiaddr);
                }
                Err(e) => {
                    let msg = format!("JoinMulticastV6 error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = (multiaddr, iface);
            false
        }
        Some(SessionCommand::LeaveMulticastV6 { multiaddr, iface }) => {
            #[cfg(feature = "udp")]
            match transport.leave_multicast_v6(multiaddr, iface) {
                Ok(()) => {
                    log::trace!("[{}] leave_multicast_v6({}) ok", session_id, multiaddr);
                }
                Err(e) => {
                    let msg = format!("LeaveMulticastV6 error: {}", e);
                    log::warn!("[{}] {}", session_id, msg);
                    let _ = tx_event.send(SessionEvent::Error { message: msg }).await;
                }
            }
            #[cfg(not(feature = "udp"))]
            let _ = (multiaddr, iface);
            false
        }
        Some(SessionCommand::Close) => {
            log::info!("[{}] Received close command", session_id);
            true
        }
        None => {
            // 命令通道已关闭（发送端 dropped）
            log::info!("[{}] Command channel closed", session_id);
            true
        }
    }
}

/// 执行一次非阻塞读取操作
///
/// # 返回值
///
/// - `true`: 应退出主循环（EOF 或致命错误）
/// - `false`: 继续循环
async fn FunEvent_do_read(
    transport: &mut SessionTransport,
    read_buf: &mut [u8],
    tx_event: &mpsc::Sender<SessionEvent>,
    app_handle: &tauri::AppHandle,
    session_id: &str,
) -> bool {
    match transport.read(read_buf).await {
        Ok(0) => {
            // EOF：对端关闭了连接
            log::info!("[{}] EOF received, closing session", session_id);
            let _ = tx_event
                .send(SessionEvent::StateChanged {
                    state: ConnectionState::Disconnected,
                })
                .await;
            true
        }
        Ok(n) => {
            let data = read_buf[..n].to_vec();

            // 发送到事件 channel（供前端 poll 读取）
            if let Err(e) = tx_event.send(SessionEvent::DataReceived { data: data.clone() }).await {
                log::warn!("[{}] Failed to send data event: {}", session_id, e);
                return true;
            }

            // 推送 Tauri 事件到前端（二进制原始数据，避免 JSON 序列化开销）
            let _ = app_handle.emit(
                &format!("transport://data/{}", session_id),
                &data[..],
            );

            false
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // 非阻塞模式下暂无数据，正常继续
            false
        }
        Err(e) => {
            let msg = format!("Read error: {}", e);
            log::warn!("[{}] {}", session_id, msg);
            let _ = tx_event.send(SessionEvent::Error { message: msg.clone() }).await;

            // 判断是否为致命错误（WouldBlock 除外均视为致命）
            if e.kind() != std::io::ErrorKind::WouldBlock {
                let _ = tx_event
                    .send(SessionEvent::StateChanged {
                        state: ConnectionState::Disconnected,
                    })
                    .await;
                true
            } else {
                false
            }
        }
    }
}

/// 获取配置类型的简短描述字符串
fn config_type_str(config: &AsyncTransportConfig) -> &'static str {
    match config {
        AsyncTransportConfig::TcpClient { .. } => "TcpClient",
        AsyncTransportConfig::TcpServer { .. } => "TcpServer",
        AsyncTransportConfig::Udp { .. } => "Udp",
        #[cfg(feature = "serial")]
        AsyncTransportConfig::Serial { .. } => "Serial",
    }
}
