/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30
 * @FilePath     : /connect-io/src/tauri_plugin/mod.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30
 * @Description  : Tauri 集成层主模块，提供 Command 封装和事件通信
 */

mod commands;
mod state;

pub use state::TransportState;

// 导出命令相关的类型，方便使用者 import
pub use commands::{
    SessionInfo,
    TransportConnectConfig,
    TransportResult,
    TransportStateInfo,
};

// 导出状态管理中的事件/命令枚举（供高级使用场景）
pub use state::{SessionCommand, SessionEvent};

/// 生成 transport 相关的 Tauri invoke handler
///
/// 在 Tauri 的 `Builder` 中调用此函数，将所有 IPC 命令注册到应用中。
///
/// # 使用示例
///
/// ```ignore
/// tauri::Builder::default()
///     .setup(|app| {
///         let state = TransportState::new();
///         app.manage(state);
///         Ok(())
///     })
///     .invoke_handler(connect_io::tauri_plugin::invoke_handler())
///     .run(tauri::generate_context!())?;
/// ```
pub fn invoke_handler() -> impl Fn(tauri::ipc::Invoke) -> bool {
    tauri::generate_handler![
        commands::FunEvent_transport_connect,
        commands::BtnEvent_transport_disconnect,
        commands::FunEvent_transport_get_state,
        commands::FunEvent_transport_write,
        commands::FunEvent_transport_read,
        commands::FunEvent_transport_send_to,
        commands::FunEvent_transport_list_sessions,
        commands::BtnEvent_transport_set_broadcast,
        commands::BtnEvent_transport_join_multicast_v4,
        commands::BtnEvent_transport_leave_multicast_v4,
        commands::BtnEvent_transport_join_multicast_v6,
        commands::BtnEvent_transport_leave_multicast_v6,
    ]
}
