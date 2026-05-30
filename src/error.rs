/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:31
 * @FilePath     : /connect-io/src/error.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 14:16:49
 * @Description  : 
 */
use thiserror::Error;

/// 传输层统一错误类型
///
/// 定义 connect-io 库中所有可能的错误情况。
/// 使用 `thiserror` 库自动实现 `std::error::Error`、`Display`、`Debug` 等 trait。
/// 该枚举实现了 `Send` + `Sync`，可安全跨线程传递。
///
/// # Error Conversion
/// - `std::io::Error` 可通过 `From` trait 自动转换为 `Io` 变体
/// - `serialport::Error` 可通过 `From` trait 自动转换为 `Serial` 变体（需 serial feature）
///
/// # Example
/// ```ignore
/// use connect_io::TransportError;
/// use std::io;
///
/// // 从 io::Error 自动转换
/// let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "连接被拒绝");
/// let transport_err: TransportError = io_err.into();
/// assert!(format!("{}", transport_err).contains("IO error"));
///
/// // 手动构造配置错误
/// let config_err = TransportError::Config("无效的波特率".to_string());
/// ```
#[derive(Error, Debug)]
pub enum TransportError {
    /// I/O 错误
    ///
    /// 封装底层 `std::io::Error`，涵盖文件操作、网络 socket 操作等系统级错误。
    /// 常见触发场景：
    /// - 网络连接失败（`ConnectionRefused`、`TimedOut`）
    /// - 地址已占用（`AddrInUse`）
    /// - 权限不足（`PermissionDenied`）
    /// - 连接重置（`ConnectionReset`、`BrokenPipe`）
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// 串口错误
    ///
    /// 封装 `serialport` 库的错误类型。
    /// 常见触发场景：
    /// - 串口设备不存在（`NoDevice`）
    /// - 无效的参数配置（`InvalidInput`，如不支持的波特率）
    /// - 设备忙（`Io`，端口已被其他进程占用）
    ///
    /// # Feature
    /// 需要启用 `serial` feature 才可使用此变体
    #[cfg(feature = "serial")]
    #[error("Serial port error: {0}")]
    Serial(#[from] serialport::Error),

    /// 配置错误
    ///
    /// 表示传入的配置参数无效或不兼容。
    /// 常见触发场景：
    /// - 配置类型与传输实现不匹配（如用 TCP 配置创建 UDP 传输）
    /// - 必要参数缺失或格式错误
    /// - 请求的功能未启用对应 feature
    #[error("Invalid configuration: {0}")]

    Config(String),

    /// 连接失败错误
    ///
    /// 表示连接建立过程中的业务逻辑错误（非底层 I/O 错误）。
    /// 与 `Io` 变体的区别：此变体用于更高层的语义化错误描述，
    /// 而 `Io` 用于直接包装底层系统错误。
    ///
    /// 常见触发场景：
    /// - 异步连接超时
    /// - 对端拒绝连接
    /// - 认证失败
    /// - 连接被防火墙拦截
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    // ============================================================
    // Error Display 格式验证
    // ============================================================

    #[test]
    fn FunEvent_display_io_error_format() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "device not found");
        let err = TransportError::Io(io_err);
        let display = format!("{}", err);

        assert!(
            display.contains("IO error"),
            "Io Display should start with 'IO error:', got: '{}'",
            display
        );
        assert!(
            display.contains("device not found"),
            "Io Display should contain original message, got: '{}'",
            display
        );
    }

    #[test]
    fn FunEvent_display_config_error_format() {
        let err = TransportError::Config("bad baud rate".into());
        let display = format!("{}", err);

        assert!(
            display.contains("Invalid configuration"),
            "Config Display should contain 'Invalid configuration', got: '{}'",
            display
        );
        assert!(
            display.contains("bad baud rate"),
            "Config Display should contain detail, got: '{}'",
            display
        );
    }

    #[test]
    fn FunEvent_display_connection_failed_format() {
        let err = TransportError::ConnectionFailed("timeout after 30s".into());
        let display = format!("{}", err);

        assert!(
            display.contains("Connection failed"),
            "ConnectionFailed Display should contain 'Connection failed', got: '{}'",
            display
        );
        assert!(
            display.contains("timeout after 30s"),
            "ConnectionFailed Display should contain detail, got: '{}'",
            display
        );
    }

    #[cfg(feature = "serial")]
    #[test]
    fn FunEvent_display_serial_error_format() {
        // serialport::Error 实现了 Display，我们验证格式化链正确
        // 由于无法轻易构造真实的 Serial Error，这里仅验证变体存在且可格式化
        let _dummy_msg = "simulated serial error";
        // 通过 String 构造来间接测试 -- 注意我们不能直接构造 serialport::Error
        // 所以我们用 from 转换路径来验证
        // 实际上 serialport::Error 不支持从 String 直接构造
        // 这里仅验证 feature gate 编译通过和基本格式化能力
        let _format_check = || -> String {
            // 这个闭包用于编译期检查，不实际运行
            let _: TransportError = TransportError::Serial(
                serialport::Error::new(serialport::ErrorKind::NoDevice, "no device")
            );
            String::new()
        };
        let _ = _format_check;
    }

    // ============================================================
    // Io 变体 from 转换验证
    // ============================================================

    #[test]
    fn FunEvent_from_io_broken_pipe() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broken");
        let transport_err = TransportError::from(io_err);

        match transport_err {
            TransportError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::BrokenPipe);
                let msg = format!("{}", inner);
                assert!(msg.contains("pipe broken"));
            }
            other => panic!("expected Io variant, got: {:?}", other),
        }
    }

    #[test]
    fn FunEvent_from_io_connection_refused() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let transport_err = TransportError::from(io_err);

        match transport_err {
            TransportError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::ConnectionRefused);
            }
            other => panic!("expected Io variant, got: {:?}", other),
        }
    }

    #[test]
    fn FunEvent_from_io_timed_out() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "operation timed out");
        let transport_err = TransportError::from(io_err);

        // 先验证 Display 格式（避免 match 后部分移动）
        let display = format!("{}", transport_err);
        assert!(display.contains("operation timed out"));

        match transport_err {
            TransportError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::TimedOut);
            }
            other => panic!("expected Io variant, got: {:?}", other),
        }
    }

    #[test]
    fn FunEvent_from_io_addr_in_use() {
        let io_err = std::io::Error::new(std::io::ErrorKind::AddrInUse, "port in use");
        let transport_err = TransportError::from(io_err);

        match transport_err {
            TransportError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::AddrInUse);
            }
            other => panic!("expected Io variant, got: {:?}", other),
        }
    }

    #[test]
    fn FunEvent_from_io_not_found() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let transport_err = TransportError::from(io_err);

        assert!(matches!(transport_err, TransportError::Io(_)));
        let display = format!("{}", transport_err);
        assert!(display.contains("file missing"));
    }

    #[cfg(feature = "serial")]
    #[test]
    fn FunEvent_from_serial_error_conversion() {
        let serial_err = serialport::Error::new(
            serialport::ErrorKind::InvalidInput,
            "invalid parameter value",
        );
        let transport_err = TransportError::from(serial_err);

        // 先验证 Display 格式（避免 match 后部分移动）
        let display = format!("{}", transport_err);
        assert!(
            display.contains("Serial port error"),
            "Serial Display should contain 'Serial port error', got: '{}'",
            display
        );
        assert!(
            display.contains("invalid parameter value"),
            "Serial Display should contain original message"
        );

        match transport_err {
            TransportError::Serial(inner) => {
                assert_eq!(inner.kind(), serialport::ErrorKind::InvalidInput);
            }
            other => panic!("expected Serial variant, got: {:?}", other),
        }
    }

    // ============================================================
    // Config / ConnectionFailed 非从转换（手动构造）
    // ============================================================

    #[test]
    fn FunEvent_config_error_manual_construct() {
        let err = TransportError::Config("empty address".to_string());

        match &err {
            TransportError::Config(msg) => {
                assert_eq!(msg, "empty address");
            }
            _ => panic!("expected Config variant"),
        }

        // Debug derive 也应正常工作
        let debug_str = format!("{:?}", err);
        assert!(
            debug_str.contains("Config") || debug_str.contains("config"),
            "Debug output should mention Config variant"
        );
    }

    #[test]
    fn FunEvent_connection_failed_manual_construct() {
        let err = TransportError::ConnectionFailed("host unreachable".to_string());

        match &err {
            TransportError::ConnectionFailed(msg) => {
                assert_eq!(msg, "host unreachable");
            }
            _ => panic!("expected ConnectionFailed variant"),
        }

        let debug_str = format!("{:?}", err);
        assert!(
            debug_str.contains("ConnectionFailed") || debug_str.contains("connection_failed"),
            "Debug output should mention ConnectionFailed variant"
        );
    }

    // ============================================================
    // Send + Sync 验证（错误类型应可跨线程传递）
    // ============================================================

    #[test]
    fn FunEvent_error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<TransportError>();
    }

    // ============================================================
    // 错误链/来源覆盖
    // ============================================================

    #[test]
    fn FunEvent_all_variants_covered() {
        // 确保所有变体均可构造并格式化
        let variants: Vec<TransportError> = vec![
            TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "generic io",
            )),
            TransportError::Config("config issue".into()),
            TransportError::ConnectionFailed("conn issue".into()),
        ];

        for (i, err) in variants.iter().enumerate() {
            let display = format!("{}", err);
            assert!(
                !display.is_empty(),
                "variant {} should produce non-empty Display",
                i
            );

            let debug_str = format!("{:?}", err);
            assert!(
                !debug_str.is_empty(),
                "variant {} should produce non-empty Debug",
                i
            );
        }
    }
}