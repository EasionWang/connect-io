/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:47:28
 * @FilePath     : /connect-io/examples/tcp_server.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:47:30
 * @Description  : 
 */
use connect_io::async_impl::{AsyncTransportConfig, AsyncTcpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let addr = "127.0.0.1:8080".parse().expect("Invalid address");
    let config = AsyncTransportConfig::TcpServer { bind_addr: addr };

    println!("Starting TCP server at {}...", addr);
    let mut transport = AsyncTcpTransport::connect(config).await?;
    println!("Client connected!");

    let mut buffer = [0u8; 1024];
    loop {
        let n = transport.read(&mut buffer).await?;
        if n == 0 {
            println!("Client disconnected.");
            break;
        }

        let received = std::str::from_utf8(&buffer[..n]).unwrap_or("Invalid UTF-8");
        println!("Received: {}", received);

        transport.write_all(&buffer[..n]).await?;
        println!("Echoed back.");
    }

    transport.close().await?;
    println!("Server closed.");

    Ok(())
}
