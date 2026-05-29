/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:37
 * @FilePath     : /connect-io/src/serial.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:33:52
 * @Description  : 
 */
use crate::{Transport, TransportConfig, TransportError};
use serialport::SerialPort;
use std::io::{Read, Write};
use std::time::Duration;

pub struct SerialTransport {
    port: Box<dyn SerialPort>,
}

impl Transport for SerialTransport {
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

    fn close(&mut self) -> Result<(), TransportError> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.port
            .set_timeout(timeout.unwrap_or(Duration::from_millis(100)))
            .map_err(|e| TransportError::Serial(e))?;
        Ok(())
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