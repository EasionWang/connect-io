/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:34:48
 * @FilePath     : /connect-io/tests/integration_test.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:34:50
 * @Description  : 
 */
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::thread;
use connect_io::{create_transport, TransportConfig};

#[test]
fn tcp_echo() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind(addr).unwrap();
    let server_addr = listener.local_addr().unwrap();

    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 1024];
        loop {
            let n = stream.read(&mut buf).unwrap();
            if n == 0 { break; }
            stream.write_all(&buf[..n]).unwrap();
        }
    });

    let config = TransportConfig::TcpClient { addr: server_addr };
    let mut transport = create_transport(config).unwrap();
    transport.write_all(b"hello, connect-io!").unwrap();
    let mut buf = [0; 1024];
    let n = transport.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"hello, connect-io!");

    transport.close().unwrap();
    handle.join().unwrap();
}