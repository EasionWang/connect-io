/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:48:22
 * @FilePath     : /connect-io/examples/serial_server.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:48:24
 * @Description  : 
 */
use connect_io::async_impl::{AsyncTransport, AsyncTransportConfig};
use connect_io::async_impl::serial::AsyncSerialTransport;
use connect_io::TransportError;
use serialport::{DataBits, StopBits, Parity, FlowControl};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let port = "/dev/ttyUSB0".to_string();
    let baud_rate = 115200;
    let config = AsyncTransportConfig::Serial {
        port: port.clone(),
        baud_rate,
        data_bits: DataBits::Eight,
        stop_bits: StopBits::One,
        parity: Parity::None,
        flow_control: FlowControl::None,
    };

    println!("Opening serial port {} at {} baud...", port, baud_rate);
    let mut transport = AsyncSerialTransport::connect(config).await?;
    println!("Serial port opened! Waiting for data...");

    let mut buffer = [0u8; 1024];
    loop {
        match transport.read(&mut buffer).await {
            Ok(n) if n > 0 => {
                let received = std::str::from_utf8(&buffer[..n]).unwrap_or("Invalid UTF-8");
                println!("Received: {}", received);

                if let Err(e) = transport.write_all(&buffer[..n]).await {
                    eprintln!("Failed to echo: {}", e);
                } else {
                    println!("Echoed back.");
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Receive error: {}", e);
            }
        }
    }
}
