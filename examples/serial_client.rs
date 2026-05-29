/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:48:08
 * @FilePath     : /connect-io/examples/serial_client.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:48:10
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
    println!("Serial port opened!");

    let message = b"Hello from async serial!";
    transport.write_all(message).await?;
    println!("Sent: {:?}", std::str::from_utf8(message));

    let mut response = [0u8; 1024];
    let n = transport.read(&mut response).await?;
    println!("Received: {:?}", std::str::from_utf8(&response[..n]));

    transport.close().await?;
    println!("Serial port closed.");

    Ok(())
}
