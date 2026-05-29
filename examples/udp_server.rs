/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:47:52
 * @FilePath     : /connect-io/examples/udp_server.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:47:54
 * @Description  : 
 */
use connect_io::async_impl::{AsyncTransport, AsyncTransportConfig};
use connect_io::async_impl::udp::AsyncUdpTransport;
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:9090".parse().expect("Invalid address");
    let config = AsyncTransportConfig::Udp {
        bind_addr,
        peer_addr: None,
    };

    println!("Starting UDP server at {}...", bind_addr);
    let mut transport = AsyncUdpTransport::connect(config).await?;
    println!("UDP server ready.");

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
            Ok(_) => {
                println!("Empty message received.");
            }
            Err(e) => {
                eprintln!("Receive error: {}", e);
            }
        }
    }
}
