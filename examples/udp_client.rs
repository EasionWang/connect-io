/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:47:40
 * @FilePath     : /connect-io/examples/udp_client.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:47:41
 * @Description  : 
 */
use connect_io::async_impl::{AsyncTransport, AsyncTransportConfig};
use connect_io::async_impl::udp::AsyncUdpTransport;
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:0".parse().expect("Invalid bind address");
    let peer_addr = "127.0.0.1:9090".parse().expect("Invalid peer address");
    let config = AsyncTransportConfig::Udp {
        bind_addr,
        peer_addr: Some(peer_addr),
    };

    println!("Connecting to UDP server at {}...", peer_addr);
    let mut transport = AsyncUdpTransport::connect(config).await?;
    println!("Connected!");

    let message = b"Hello from async UDP client!";
    transport.write_all(message).await?;
    println!("Sent: {:?}", std::str::from_utf8(message));

    let mut response = [0u8; 1024];
    let n = transport.read(&mut response).await?;
    println!("Received: {:?}", std::str::from_utf8(&response[..n]));

    transport.close().await?;
    println!("Connection closed.");

    Ok(())
}
