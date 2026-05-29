/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:47:16
 * @FilePath     : /connect-io/examples/tcp_client.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:47:18
 * @Description  : 
 */
use connect_io::async_impl::{AsyncTransportConfig, AsyncTcpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let addr = "127.0.0.1:8080".parse().expect("Invalid address");
    let config = AsyncTransportConfig::TcpClient { addr };

    println!("Connecting to TCP server at {}...", addr);
    let mut transport = AsyncTcpTransport::connect(config).await?;
    println!("Connected!");

    let message = b"Hello from async TCP client!";
    transport.write_all(message).await?;
    println!("Sent: {:?}", std::str::from_utf8(message));

    let mut response = [0u8; 1024];
    let n = transport.read(&mut response).await?;
    println!("Received: {:?}", std::str::from_utf8(&response[..n]));

    transport.close().await?;
    println!("Connection closed.");

    Ok(())
}
