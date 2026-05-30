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

/// 注册所有 transport 相关的 Tauri 命令到应用
///
/// 在 Tauri 的 `setup` 或 `Builder` 中调用此函数，
/// 将所有 IPC 命令注册到 `tauri::App` 中。
///
/// # 使用示例
///
/// ```ignore
/// tauri::Builder::default()
///     .setup(|app| {
///         let state = TransportState::new();
///         app.manage(state);
///         tauri_plugin::register_commands(app)?;
///         Ok(())
///     })
/// ```
///
/// # 参数
///
/// - `app`: 可变的 Tauri 应用引用
///
/// # 返回
///
/// - `Ok(())`: 所有命令注册成功
/// - `Err(String)`: 注册失败（通常不会发生）
pub fn register_commands(app: &mut tauri::App) -> Result<(), String> {
    // ============================================================
    // 连接管理命令
    // ============================================================
    app.register_tauri_command(
        "transport_connect",
        commands::FunEvent_transport_connect,
    )
    .map_err(|e| format!("Failed to register transport_connect: {}", e))?;

    app.register_tauri_command(
        "transport_disconnect",
        commands::BtnEvent_transport_disconnect,
    )
    .map_err(|e| format!("Failed to register transport_disconnect: {}", e))?;

    app.register_tauri_command(
        "transport_get_state",
        commands::FunEvent_transport_get_state,
    )
    .map_err(|e| format!("Failed to register transport_get_state: {}", e))?;

    // ============================================================
    // 数据传输命令
    // ============================================================
    app.register_tauri_command("transport_write", commands::FunEvent_transport_write)
        .map_err(|e| format!("Failed to register transport_write: {}", e))?;

    app.register_tauri_command("transport_read", commands::FunEvent_transport_read)
        .map_err(|e| format!("Failed to register transport_read: {}", e))?;

    app.register_tauri_command(
        "transport_send_to",
        commands::FunEvent_transport_send_to,
    )
    .map_err(|e| format!("Failed to register transport_send_to: {}", e))?;

    // ============================================================
    // 会话管理命令
    // ============================================================
    app.register_tauri_command(
        "transport_list_sessions",
        commands::FunEvent_transport_list_sessions,
    )
    .map_err(|e| format!("Failed to register transport_list_sessions: {}", e))?;

    log::info!("All transport commands registered successfully");

    Ok(())
}
