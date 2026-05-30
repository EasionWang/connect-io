/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:37
 * @FilePath     : /connect-io/src/serial.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 14:15:12
 * @Description  : 
 */
use crate::{ConnectionState, Transport, TransportConfig, TransportError};
use serialport::SerialPort;
use std::io::{Read, Write};
use std::time::Duration;

/// 串口传输实现（同步）
///
/// 封装 `serialport` 库的 `SerialPort` trait 对象，
/// 提供基于 RS-232/RS-485 串口的字节流传输能力。
///
/// # Characteristics
/// - **协议**: 串口通信（物理层点对点）
/// - **连接模型**: 打开端口即视为已连接（无握手概念）
/// - **默认超时**: 100 毫秒
/// - **数据格式**: 可配置波特率、数据位、停止位、校验位、流控制
///
/// # Platform Support
/// - **Linux**: `/dev/ttyUSB0`, `/dev/ttyACM0`, `/dev/ttyS0`
/// - **macOS**: `/dev/cu.usbserial`, `/dev/tty.usbmodem`
/// - **Windows**: `COM1`, `COM3`
///
/// # Example
/// ```ignore
/// use connect_io::{SerialTransport, Transport, TransportConfig};
/// use serialport::{DataBits, StopBits, Parity, FlowControl};
///
/// let config = TransportConfig::Serial {
///     port: "/dev/ttyUSB0".to_string(),
///     baud_rate: 115200,
///     data_bits: DataBits::Eight,
///     stop_bits: StopBits::One,
///     parity: Parity::None,
///     flow_control: FlowControl::None,
/// };
/// let mut transport = SerialTransport::connect(config)?;
///
/// transport.write_all(b"AT\r\n")?;
/// transport.flush()?;
///
/// let mut buf = [0u8; 256];
/// let n = transport.read(&mut buf)?;
///
/// transport.close()?;
/// # Ok::<(), connect_io::TransportError>(())
/// ```
pub struct SerialTransport {
    port: Box<dyn SerialPort>,
}

impl Transport for SerialTransport {
    /// 打开串口设备
    ///
    /// 根据配置参数打开指定的串口设备并设置通信参数。
    /// 成功后串口即可进行数据收发。
    ///
    /// # Arguments
    /// * `config` - 必须为 `Serial` 变体，包含端口路径和通信参数
    ///
    /// # Returns
    /// 成功返回已打开的 `SerialTransport` 实例
    ///
    /// # Errors
    /// - `Config("Expected Serial config")` - 配置类型不是 Serial
    /// - `Serial(NoDevice)` - 指定的串口设备不存在
    /// - `Serial(InvalidInput)` - 无效的参数配置（如不支持的波特率）
    /// - `Serial(Io)` - 设备忙或权限不足
    fn connect(config: TransportConfig) -> Result<Self, TransportError> {
        if let TransportConfig::Serial {
            port,
            baud_rate,
            data_bits,
            stop_bits,
            parity,
            flow_control,
        } = config
        {
            let builder = serialport::new(&port, baud_rate)
                .data_bits(data_bits)
                .stop_bits(stop_bits)
                .parity(parity)
                .flow_control(flow_control)
                .timeout(Duration::from_millis(100));
            let port = builder.open()?;
            Ok(SerialTransport { port })
        } else {
            Err(TransportError::Config("Expected Serial config".into()))
        }
    }

    /// 关闭串口
    ///
    /// 串口无显式关闭操作，此方法仅做兼容性处理，始终返回成功。
    /// 底层串口资源在 `SerialTransport` 被 drop 时自动释放。
    ///
    /// # Returns
    /// 始终返回 `Ok(())`
    fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    /// 检查串口是否可用
    ///
    /// 串口打开后即视为已连接，始终返回 true。
    /// 如需检测设备是否实际在线，需通过实际通信验证。
    ///
    /// # Returns
    /// 始终返回 `true`
    fn is_connected(&self) -> bool {
        true
    }

    /// 设置串口读写超时时间
    ///
    /// 配置串口操作的超时时间。
    /// 如果传入 None，将使用默认超时 100 毫秒。
    ///
    /// # Arguments
    /// * `timeout` - 超时时间，None 表示使用默认值（100ms）
    ///
    /// # Returns
    /// - `Ok(())` - 设置成功
    /// - `Err(Serial(...))` - 串口参数设置失败
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.port
            .set_timeout(timeout.unwrap_or(Duration::from_millis(100)))
            .map_err(TransportError::Serial)?;
        Ok(())
    }

    /// 获取串口连接状态
    ///
    /// 串口无连接概念，打开端口即视为已连接。
    ///
    /// # Returns
    /// 始终返回 `ConnectionState::Connected`
    fn connection_state(&self) -> ConnectionState {
        // 串口无连接概念，打开即视为已连接
        ConnectionState::Connected
    }
}

impl Read for SerialTransport {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.port.read(buf)
    }
}

impl Write for SerialTransport {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.port.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.port.flush()
    }
}