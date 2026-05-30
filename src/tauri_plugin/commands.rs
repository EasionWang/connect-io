/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30
 * @FilePath     : /connect-io/src/tauri_plugin/commands.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30
 * @Description  : Tauri 集成层命令定义，提供前端可调用的 IPC 接口
 */
#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};

use crate::async_impl::AsyncTransportConfig;
use crate::tauri_plugin::state::TransportState;

// ============================================================
// Serde 配置与结果结构体
// ============================================================

/// 前端传入的传输连接配置（serde 可反序列化）
///
/// 使用 `tag = "type"` 实现内部 tagged enum，前端 JSON 格式为：
/// ```json
/// { "type": "TcpClient", "addr": "127.0.0.1:8080" }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransportConnectConfig {
    /// TCP 客户端连接到指定地址
    TcpClient { addr: String },
    /// TCP 服务端绑定到指定地址（accept 后创建 session）
    TcpServer { bind_addr: String },
    /// UDP socket（可选 peer_addr 用于 connect 模式）
    Udp {
        bind_addr: String,
        #[serde(default)]
        peer_addr: Option<String>,
    },
    /// 串口连接
    Serial {
        port: String,
        #[serde(default = "default_baud_rate")]
        baud_rate: u32,
        #[serde(default = "default_data_bits")]
        data_bits: u8,
        #[serde(default = "default_stop_bits")]
        stop_bits: u8,
        #[serde(default = "default_parity")]
        parity: String,
        #[serde(default = "default_flow_control")]
        flow_control: String,
    },
}

/// 默认波特率：9600
fn default_baud_rate() -> u32 {
    9600
}

/// 默认数据位：8
fn default_data_bits() -> u8 {
    8
}

/// 默认停止位：1（对应 StopBits::One）
fn default_stop_bits() -> u8 {
    1
}

/// 默认校验位：None
fn default_parity() -> String {
    "None".to_string()
}

/// 默认流控制：None
fn default_flow_control() -> String {
    "None".to_string()
}

/// 连接操作的统一返回结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportResult {
    /// 操作是否成功
    pub success: bool,
    /// 关联的会话 ID
    pub session_id: String,
    /// 人类可读的消息
    pub message: String,
}

/// 会话状态信息（供前端查询）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportStateInfo {
    /// 会话 ID
    pub session_id: String,
    /// 是否已连接
    pub connected: bool,
    /// 连接状态字符串表示
    pub connection_state: String,
    /// 本地地址（如有）
    pub local_addr: Option<String>,
    /// 对端地址（如有）
    pub peer_addr: Option<String>,
}

/// 会话概要信息（用于列表展示）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    /// 会话 ID
    pub session_id: String,
    /// 协议类型："tcp" | "udp" | "serial"
    pub protocol: String,
    /// 角色类型："client" | "server"
    pub role: String,
}

impl TryFrom<TransportConnectConfig> for AsyncTransportConfig {
    type Error = String;

    fn try_from(value: TransportConnectConfig) -> Result<Self, Self::Error> {
        match value {
            TransportConnectConfig::TcpClient { addr } => {
                let parsed: std::net::SocketAddr =
                    addr.parse().map_err(|e| format!("Invalid TCP address '{}': {}", addr, e))?;
                Ok(AsyncTransportConfig::TcpClient { addr: parsed })
            }
            TransportConnectConfig::TcpServer { bind_addr } => {
                let parsed: std::net::SocketAddr = bind_addr
                    .parse()
                    .map_err(|e| format!("Invalid bind address '{}': {}", bind_addr, e))?;
                Ok(AsyncTransportConfig::TcpServer { bind_addr: parsed })
            }
            TransportConnectConfig::Udp {
                bind_addr,
                peer_addr,
            } => {
                let parsed_bind: std::net::SocketAddr = bind_addr
                    .parse()
                    .map_err(|e| format!("Invalid UDP bind address '{}': {}", bind_addr, e))?;

                let parsed_peer = match peer_addr {
                    Some(ref addr_str) => Some(
                        addr_str
                            .parse::<std::net::SocketAddr>()
                            .map_err(|e| format!("Invalid UDP peer address '{}': {}", addr_str, e))?,
                    ),
                    None => None,
                };

                Ok(AsyncTransportConfig::Udp {
                    bind_addr: parsed_bind,
                    peer_addr: parsed_peer,
                })
            }
            #[cfg(feature = "serial")]
            TransportConnectConfig::Serial {
                port,
                baud_rate,
                data_bits,
                stop_bits,
                parity,
                flow_control,
            } => {
                let data_bits = FunEvent_parse_data_bits(data_bits)?;
                let stop_bits = FunEvent_parse_stop_bits(stop_bits)?;
                let parity = FunEvent_parse_parity(&parity)?;
                let flow_control = FunEvent_parse_flow_control(&flow_control)?;

                Ok(AsyncTransportConfig::Serial {
                    port,
                    baud_rate,
                    data_bits,
                    stop_bits,
                    parity,
                    flow_control,
                })
            }
            #[cfg(not(feature = "serial"))]
            TransportConnectConfig::Serial { .. } => {
                Err("Serial feature not enabled".to_string())
            }
        }
    }
}

// ============================================================
// 串口参数解析辅助函数
// ============================================================

#[cfg(feature = "serial")]
fn FunEvent_parse_data_bits(value: u8) -> Result<serialport::DataBits, String> {
    match value {
        5 => Ok(serialport::DataBits::Five),
        6 => Ok(serialport::DataBits::Six),
        7 => Ok(serialport::DataBits::Seven),
        8 => Ok(serialport::DataBits::Eight),
        other => Err(format!(
            "Invalid data_bits value {}, must be 5-8",
            other
        )),
    }
}

#[cfg(feature = "serial")]
fn FunEvent_parse_stop_bits(value: u8) -> Result<serialport::StopBits, String> {
    match value {
        1 => Ok(serialport::StopBits::One),
        2 => Ok(serialport::StopBits::Two),
        other => Err(format!(
            "Invalid stop_bits value {}, must be 1 or 2",
            other
        )),
    }
}

#[cfg(feature = "serial")]
fn FunEvent_parse_parity(value: &str) -> Result<serialport::Parity, String> {
    match value.to_uppercase().as_str() {
        "NONE" | "N" => Ok(serialport::Parity::None),
        "ODD" | "O" => Ok(serialport::Parity::Odd),
        "EVEN" | "E" => Ok(serialport::Parity::Even),
        other => Err(format!("Invalid parity '{}', expected None/Odd/Even", other)),
    }
}

#[cfg(feature = "serial")]
fn FunEvent_parse_flow_control(value: &str) -> Result<serialport::FlowControl, String> {
    match value.to_uppercase().as_str() {
        "NONE" => Ok(serialport::FlowControl::None),
        "RTS/CTS" | "HARDWARE" => Ok(serialport::FlowControl::Hardware),
        "XON/XOFF" | "SOFTWARE" => Ok(serialport::FlowControl::Software),
        other => Err(format!(
            "Invalid flow_control '{}', expected None/Hardware/Software",
            other
        )),
    }
}

// ============================================================
// Tauri Commands —— 连接管理
// ============================================================

/// 创建传输会话并建立连接
///
/// 根据前端传入的配置创建异步传输实例，启动后台读写任务。
/// 成功后前端可通过 `session_id` 进行数据收发。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: 前端指定的会话唯一标识
/// - `config`: 连接配置（TCP/UDP/Serial）
///
/// # 返回
///
/// - `Ok(TransportResult)`: 包含成功状态和消息
/// - `Err(String)`: 参数无效或连接失败
#[tauri::command]
pub async fn FunEvent_transport_connect(
    state: tauri::State<'_, TransportState>,
    app_handle: tauri::AppHandle,
    session_id: String,
    config: TransportConnectConfig,
) -> Result<TransportResult, String> {
    // 验证 session_id 非空
    if session_id.trim().is_empty() {
        return Ok(TransportResult {
            success: false,
            session_id,
            message: "session_id must not be empty".to_string(),
        });
    }

    // 转换配置为内部 AsyncTransportConfig
    let async_config = AsyncTransportConfig::try_from(config)?;

    // 创建会话（内部启动后台任务）
    state.create_session(session_id.clone(), async_config, app_handle)?;

    Ok(TransportResult {
        success: true,
        session_id,
        message: "Session created and connecting".to_string(),
    })
}

/// 断开指定会话的连接并释放资源
///
/// 向后台任务发送 Close 命令，从管理器中移除该会话。
/// 后台任务将执行 shutdown 并退出事件循环。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: 要断开的会话 ID
///
/// # 返回
///
/// - `Ok(())`: 断开请求已发送
/// - `Err(String)`: session 不存在
#[tauri::command]
pub async fn BtnEvent_transport_disconnect(
    state: tauri::State<'_, TransportState>,
    session_id: String,
) -> Result<(), String> {
    state.close(&session_id)
}

/// 查询指定会话的当前连接状态
///
/// 通过检查命令 channel 存活状态判断连接是否活跃。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: 要查询的会话 ID
///
/// # 返回
///
/// - `Ok(TransportStateInfo)`: 包含连接状态、地址等信息
/// - `Err(String)`: session 不存在
#[tauri::command]
pub async fn FunEvent_transport_get_state(
    state: tauri::State<'_, TransportState>,
    session_id: String,
) -> Result<TransportStateInfo, String> {
    let conn_state = state.get_state(&session_id)?;
    let (local_addr, peer_addr) = state
        .list_sessions()
        .ok()
        .and_then(|sessions| {
            sessions
                .iter()
                .find(|(id, _, _)| id == &session_id)
                .map(|_| (None, None))
        })
        .unwrap_or((None, None));

    let (state_str, connected) = match conn_state {
        crate::ConnectionState::Connected => ("Connected".to_string(), true),
        crate::ConnectionState::Disconnected => ("Disconnected".to_string(), false),
        crate::ConnectionState::Connecting => ("Connecting".to_string(), false),
        crate::ConnectionState::Unknown => ("Unknown".to_string(), false),
    };

    Ok(TransportStateInfo {
        session_id,
        connected,
        connection_state: state_str,
        local_addr,
        peer_addr,
    })
}

// ============================================================
// Tauri Commands —— 数据传输
// ============================================================

/// 向指定会话写入二进制数据
///
/// 数据通过 mpsc channel 发送给后台任务执行异步写入。
/// 前端应使用 Uint8Array 或 Base64 编码传递二进制数据。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: 目标会话 ID
/// - `data`: 待写入的二进制数据（原始字节）
///
/// # 返回
///
/// - `Ok(usize)`: 写入的字节数
/// - `Err(String)`: session 不存在或写入失败
#[tauri::command]
pub async fn FunEvent_transport_write(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    data: Vec<u8>,
) -> Result<usize, String> {
    let len = data.len();
    state.write(&session_id, data)?;
    Ok(len)
}

/// 从指定会话读取可用数据（非阻塞轮询模式）
///
/// 尝试从 event channel 获取所有已缓存的数据块。
/// 无数据时返回空列表。前端应配合定时器或事件监听使用。
///
/// 推荐优先使用 Tauri 事件监听 `transport://data/{session_id}`
/// 获取实时推送的数据，本方法作为补充手段。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: 目标会话 ID
///
/// # 返回
///
/// - `Ok(Vec<u8>)`: 拼接后的所有可用数据（空 Vec 表示无数据）
/// - `Err(String)`: session 不存在
#[tauri::command]
pub async fn FunEvent_transport_read(
    state: tauri::State<'_, TransportState>,
    session_id: String,
) -> Result<Vec<u8>, String> {
    let chunks = state.read(&session_id)?;
    // 将所有数据块拼接为单个 Vec<u8>
    let total_size: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_size);
    for chunk in chunks {
        result.extend_from_slice(&chunk);
    }
    Ok(result)
}

// ============================================================
// UDP 广播 / 组播命令
// ============================================================

/// 设置 UDP 会话的广播模式
///
/// 启用后，可以向广播地址（如 255.255.255.255）发送数据报。
///
/// # 参数
/// - `session_id`: UDP 会话 ID
/// - `flag`: true 启用广播，false 禁用
///
/// # 返回
/// - `Ok(())`: 设置成功
/// - `Err(String)`: session 不存在或非 UDP 会话
#[tauri::command]
pub async fn BtnEvent_transport_set_broadcast(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    flag: bool,
) -> Result<(), String> {
    state.set_broadcast(&session_id, flag)
}

/// 加入 IPv4 组播组
///
/// # 参数
/// - `session_id`: UDP 会话 ID
/// - `multiaddr`: 组播地址（如 "224.0.0.1"）
/// - `iface`: 本地接口地址（如 "0.0.0.0" 表示任意接口）
///
/// # 返回
/// - `Ok(())`: 加入成功
/// - `Err(String)`: 加入失败
#[tauri::command]
pub async fn BtnEvent_transport_join_multicast_v4(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    multiaddr: String,
    iface: String,
) -> Result<(), String> {
    let multiaddr = multiaddr.parse::<std::net::Ipv4Addr>()
        .map_err(|e| format!("Invalid multicast address: {}", e))?;
    let iface = iface.parse::<std::net::Ipv4Addr>()
        .map_err(|e| format!("Invalid interface address: {}", e))?;
    state.join_multicast_v4(&session_id, multiaddr, iface)
}

/// 离开 IPv4 组播组
///
/// # 参数
/// - `session_id`: UDP 会话 ID
/// - `multiaddr`: 组播地址
/// - `iface`: 本地接口地址
///
/// # 返回
/// - `Ok(())`: 离开成功
/// - `Err(String)`: 离开失败
#[tauri::command]
pub async fn BtnEvent_transport_leave_multicast_v4(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    multiaddr: String,
    iface: String,
) -> Result<(), String> {
    let multiaddr = multiaddr.parse::<std::net::Ipv4Addr>()
        .map_err(|e| format!("Invalid multicast address: {}", e))?;
    let iface = iface.parse::<std::net::Ipv4Addr>()
        .map_err(|e| format!("Invalid interface address: {}", e))?;
    state.leave_multicast_v4(&session_id, multiaddr, iface)
}

/// 加入 IPv6 组播组
///
/// # 参数
/// - `session_id`: UDP 会话 ID
/// - `multiaddr`: IPv6 组播地址（如 "ff02::1"）
/// - `iface`: 接口索引（0 表示默认接口）
///
/// # 返回
/// - `Ok(())`: 加入成功
/// - `Err(String)`: 加入失败
#[tauri::command]
pub async fn BtnEvent_transport_join_multicast_v6(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    multiaddr: String,
    iface: u32,
) -> Result<(), String> {
    let multiaddr = multiaddr.parse::<std::net::Ipv6Addr>()
        .map_err(|e| format!("Invalid IPv6 multicast address: {}", e))?;
    state.join_multicast_v6(&session_id, multiaddr, iface)
}

/// 离开 IPv6 组播组
///
/// # 参数
/// - `session_id`: UDP 会话 ID
/// - `multiaddr`: IPv6 组播地址
/// - `iface`: 接口索引
///
/// # 返回
/// - `Ok(())`: 离开成功
/// - `Err(String)`: 离开失败
#[tauri::command]
pub async fn BtnEvent_transport_leave_multicast_v6(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    multiaddr: String,
    iface: u32,
) -> Result<(), String> {
    let multiaddr = multiaddr.parse::<std::net::Ipv6Addr>()
        .map_err(|e| format!("Invalid IPv6 multicast address: {}", e))?;
    state.leave_multicast_v6(&session_id, multiaddr, iface)
}

/// UDP 无连接模式：向指定地址发送数据
///
/// 仅对 UDP 类型的会话有效。对于已 connect 的 UDP socket，
/// 请改用 `FunEvent_transport_write`。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
/// - `session_id`: UDP 会话 ID
/// - `data`: 待发送的二进制数据
/// - `addr`: 目标地址（如 "192.168.1.1:5000"）
///
/// # 返回
///
/// - `Ok(usize)`: 发送的字节数
/// - `Err(String)`: session 不存在或发送失败
#[tauri::command]
pub async fn FunEvent_transport_send_to(
    state: tauri::State<'_, TransportState>,
    session_id: String,
    data: Vec<u8>,
    addr: String,
) -> Result<usize, String> {
    let parsed: std::net::SocketAddr =
        addr.parse().map_err(|e| format!("Invalid address '{}': {}", addr, e))?;
    let len = data.len();
    state.send_to(&session_id, data, parsed)?;
    Ok(len)
}

// ============================================================
// Tauri Commands —— 会话管理
// ============================================================

/// 列出所有当前活跃的传输会话
///
/// 返回每个会话的基本信息（ID、协议类型、角色），
/// 前端可用于构建会话管理 UI。
///
/// # 参数
///
/// - `state`: Tauri 管理的 TransportState 全局状态
///
/// # 返回
///
/// - `Ok(Vec<SessionInfo>)`: 活跃会话列表（可能为空）
#[tauri::command]
pub async fn FunEvent_transport_list_sessions(
    state: tauri::State<'_, TransportState>,
) -> Result<Vec<SessionInfo>, String> {
    let raw_list = state.list_sessions()?;

    let result = raw_list
        .into_iter()
        .map(|(id, protocol, role)| SessionInfo {
            session_id: id,
            protocol,
            role,
        })
        .collect();

    Ok(result)
}
