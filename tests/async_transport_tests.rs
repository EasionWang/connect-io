/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-30 10:00:00
 * @FilePath     : /connect-io/tests/async_transport_tests.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-30 14:12:26
 * @Description  : 异步传输层全面测试套件
 *               覆盖 AsyncTcpTransport / AsyncUdpTransport 的连接、读写、状态查询、错误路径
 */

use std::net::SocketAddr;
use std::time::Duration;

#[cfg(feature = "async")]
use connect_io::{
    async_impl::{create_async_transport, AsyncTcpTransport, AsyncTransport, AsyncTransportConfig, AsyncUdpTransport},
    TransportError,
};

#[cfg(not(feature = "async"))]
use connect_io::TransportError;

// ============================================================
// AsyncTcpTransport 客户端测试
// ============================================================

#[cfg(all(feature = "tcp", feature = "async"))]
mod async_tcp_client_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 辅助：启动异步 echo 服务端，返回地址
    async fn spawn_async_echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("async TCP bind failed");
        let addr = listener.local_addr().expect("local_addr failed");

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((mut stream, _)) => {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            loop {
                                match stream.read(&mut buf).await {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        if stream.write_all(&buf[..n]).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        addr
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_connect_and_echo() {
        let addr = spawn_async_echo_server().await;

        // 短暂等待服务端就绪
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport =
            AsyncTcpTransport::connect(config).await.expect("async TCP connect failed");

        assert!(
            transport.is_connected(),
            "is_connected should be true after connect"
        );

        let payload = b"async-tcp-payload";
        transport.write_all(payload).await.expect("async write failed");

        let mut buf = [0u8; 256];
        let n = transport.read(&mut buf).await.expect("async read failed");
        assert_eq!(&buf[..n], payload, "echo data mismatch");

        transport.close().await.expect("close failed");
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_local_peer_addr() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let transport = AsyncTcpTransport::connect(config).await.expect("connect failed");

        let local = transport.local_addr();
        assert!(local.is_some(), "local_addr should be Some");

        let peer = transport.peer_addr();
        assert!(peer.is_some(), "peer_addr should be Some");
        assert_eq!(peer.unwrap(), addr, "peer_addr should match server address");
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_set_timeout_no_panic() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = AsyncTcpTransport::connect(config).await.expect("connect failed");

        // AsyncTcpTransport 的 set_timeout 是空实现（返回 Ok），验证不 panic 即可
        let result = transport.set_timeout(Some(Duration::from_secs(1)));
        assert!(result.is_ok(), "set_timeout should not fail");

        let result = transport.set_timeout(None);
        assert!(result.is_ok(), "set_timeout(None) should not fail");
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_close_sets_disconnected() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = AsyncTcpTransport::connect(config).await.expect("connect failed");
        assert!(transport.is_connected());

        transport.close().await.expect("close failed");
        assert!(
            !transport.is_connected(),
            "is_connected should be false after close (stream taken)"
        );
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_close_then_read_fails() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = AsyncTcpTransport::connect(config).await.expect("connect failed");
        transport.close().await.expect("close failed");

        // close 后 stream 被 take()，后续读应失败
        let mut buf = [0u8; 64];
        let result = transport.read(&mut buf).await;
        assert!(result.is_err(), "read after close should fail");
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_close_then_write_fails() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = AsyncTcpTransport::connect(config).await.expect("connect failed");
        transport.close().await.expect("close failed");

        let result = transport.write(b"data").await;
        assert!(result.is_err(), "write after close should fail");
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_wrong_config_rejects() {
        let bad_config = AsyncTransportConfig::Udp {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peer_addr: None,
        };

        let result = AsyncTcpTransport::connect(bad_config).await;
        assert!(result.is_err(), "should reject non-TCP config");
        match result.unwrap_err() {
            TransportError::Config(msg) => {
                assert!(
                    msg.contains("Expected TCP config"),
                    "error message should mention expected TCP config"
                );
            }
            other => panic!("expected Config error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_new_is_not_connected() {
        let transport = AsyncTcpTransport::new();
        assert!(
            !transport.is_connected(),
            "freshly created AsyncTcpTransport should not be connected"
        );
    }

    #[tokio::test]
    async fn FunEvent_async_tcp_multi_write_read_cycle() {
        let addr = spawn_async_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = AsyncTcpTransport::connect(config).await.expect("connect failed");

        for i in 0..10u8 {
            let msg = format!("msg-{:03}", i);
            transport.write_all(msg.as_bytes()).await.expect("write failed");

            let mut buf = [0u8; 64];
            let n = transport.read(&mut buf).await.expect("read failed");
            assert_eq!(&buf[..n], msg.as_bytes(), "cycle {} mismatch", i);
        }

        transport.close().await.expect("close failed");
    }
}

// ============================================================
// AsyncTcpTransport 服务端测试
// ============================================================

#[cfg(all(feature = "tcp", feature = "async"))]
mod async_tcp_server_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn FunEvent_async_tcp_server_accept_once() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        // 启动客户端线程（在 tokio task 中）
        let client_addr = bind_addr; // 将由 server 分配实际端口

        // 先绑定 listener 获取实际地址
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("bind failed");
        let actual_addr = listener.local_addr().expect("local_addr failed");

        // 在后台启动客户端
        let client_handle = tokio::spawn(async move {
            let mut stream = TcpStream::connect(actual_addr)
                .await
                .expect("client connect failed");
            stream
                .write_all(b"async-server-test")
                .await
                .expect("client write failed");
            stream.shutdown().await.expect("client shutdown failed");
        });

        // 使用 TcpServer 配置接受连接
        let server_config = AsyncTransportConfig::TcpServer {
            bind_addr: actual_addr,
        };
        let mut server_transport = AsyncTcpTransport::connect(server_config)
            .await
            .expect("server accept failed");

        assert!(
            server_transport.is_connected(),
            "server should be connected after accept"
        );

        let mut buf = [0u8; 64];
        let n = server_transport.read(&mut buf).await.expect("server read failed");
        assert_eq!(&buf[..n], b"async-server-test", "server received wrong data");

        server_transport.close().await.expect("server close failed");
        client_handle.await.expect("client task panicked");
    }
}

// ============================================================
// AsyncUdpTransport 连接模式测试
// ============================================================

#[cfg(all(feature = "udp", feature = "async"))]
mod async_udp_connected_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UdpSocket;

    /// 辅助：启动异步 UDP echo 服务端
    async fn spawn_async_udp_echo_server() -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("UDP bind failed");
        let addr = socket.local_addr().expect("local_addr failed");

        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, src)) => {
                        if socket.send_to(&buf[..len], src).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        addr
    }

    #[tokio::test]
    async fn FunEvent_async_udp_connected_echo() {
        let server_addr = spawn_async_udp_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: client_bind,
            peer_addr: Some(server_addr),
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("UDP connected mode connect failed");

        assert!(
            transport.is_connected(),
            "is_connected should be true in connected mode"
        );

        let payload = b"async-udp-connected";
        transport.write_all(payload).await.expect("write failed");

        let mut buf = [0u8; 128];
        let n = transport.read(&mut buf).await.expect("read failed");
        assert_eq!(&buf[..n], payload, "echo data mismatch");

        transport.close().await.expect("close failed");
    }

    #[tokio::test]
    async fn FunEvent_async_udp_connected_addresses() {
        let server_addr = spawn_async_udp_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: client_bind,
            peer_addr: Some(server_addr),
        };

        let transport = AsyncUdpTransport::connect(config)
            .await
            .expect("connect failed");

        let local = transport.local_addr();
        assert!(local.is_some(), "local_addr should be Some");

        let peer = transport.peer_addr();
        assert!(peer.is_some(), "peer_addr should be Some");
        assert_eq!(peer.unwrap(), server_addr);
    }

    #[tokio::test]
    async fn FunEvent_async_udp_multi_packet_echo() {
        let server_addr = spawn_async_udp_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: client_bind,
            peer_addr: Some(server_addr),
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("connect failed");

        for i in 0..5u8 {
            let packet = [i; 48];
            transport.write_all(&packet).await.expect("write packet failed");

            let mut buf = [0u8; 64];
            let n = transport.read(&mut buf).await.expect("read packet failed");
            assert_eq!(&buf[..n], &packet[..], "packet {} mismatch", i);
        }

        transport.close().await.expect("close failed");
    }

    #[tokio::test]
    async fn FunEvent_async_udp_set_timeout_no_panic() {
        let server_addr = spawn_async_udp_echo_server().await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: client_bind,
            peer_addr: Some(server_addr),
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("connect failed");

        let result = transport.set_timeout(Some(Duration::from_secs(2)));
        assert!(result.is_ok(), "set_timeout should succeed");
    }
}

// ============================================================
// AsyncUdpTransport 未连接模式测试
// ============================================================

#[cfg(all(feature = "udp", feature = "async"))]
mod async_udp_unconnected_tests {
    use super::*;
    use std::io::ErrorKind;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn FunEvent_async_udp_unconnected_is_not_connected() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr: None,
        };

        let transport = AsyncUdpTransport::connect(config)
            .await
            .expect("unconnected bind should succeed");

        assert!(
            !transport.is_connected(),
            "is_connected must be false in unconnected mode"
        );
    }

    #[tokio::test]
    async fn FunEvent_async_udp_unconnected_write_returns_error() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr: None,
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("unconnected bind succeeded");

        let result = transport.write(b"data").await;
        assert!(result.is_err(), "write on unconnected UDP should return error");

        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::NotConnected,
            "error kind should be NotConnected"
        );
    }

    #[tokio::test]
    async fn FunEvent_async_udp_unconnected_read_returns_error() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr: None,
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("unconnected bind succeeded");

        let mut buf = [0u8; 64];
        let result = transport.read(&mut buf).await;
        assert!(result.is_err(), "read on unconnected UDP should return error");

        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::NotConnected,
            "error kind should be NotConnected"
        );
    }

    #[tokio::test]
    async fn FunEvent_async_udp_unconnected_peer_addr_none() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr: None,
        };

        let transport = AsyncUdpTransport::connect(config)
            .await
            .expect("unconnected bind succeeded");

        let peer = transport.peer_addr();
        assert!(
            peer.is_none(),
            "peer_addr should be None in unconnected mode"
        );

        let local = transport.local_addr();
        assert!(local.is_some(), "local_addr should still work");
    }

    #[tokio::test]
    async fn FunEvent_async_udp_unconnected_close_succeeds() {
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr,
            peer_addr: None,
        };

        let mut transport = AsyncUdpTransport::connect(config)
            .await
            .expect("unconnected bind succeeded");

        let result = transport.close().await;
        assert!(result.is_ok(), "close on unconnected UDP should succeed");
    }
}

// ============================================================
// create_async_transport 工厂函数测试
// ============================================================

#[cfg(feature = "async")]
mod async_factory_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[cfg(feature = "tcp")]
    #[tokio::test]
    async fn FunEvent_async_factory_creates_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind failed");
        let addr = listener.local_addr().expect("local_addr failed");

        tokio::spawn(async move {
            if let Ok((mut s, _)) = listener.accept().await {
                let mut buf = [0; 256];
                if let Ok(n) = s.read(&mut buf).await {
                    let _ = s.write_all(&buf[..n]).await;
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let config = AsyncTransportConfig::TcpClient { addr };
        let mut transport = create_async_transport(config)
            .expect("factory create async TCP failed");

        transport.write_all(b"factory-async-tcp").await.expect("write failed");
        let mut buf = [0; 64];
        let n = transport.read(&mut buf).await.expect("read failed");
        assert_eq!(&buf[..n], b"factory-async-tcp");

        transport.close().await.expect("close failed");
    }

    #[cfg(feature = "udp")]
    #[tokio::test]
    async fn FunEvent_async_factory_creates_udp() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("UDP bind failed");
        let addr = socket.local_addr().expect("local_addr failed");

        tokio::spawn(async move {
            let mut buf = [0; 256];
            while let Ok((n, src)) = socket.recv_from(&mut buf).await {
                let _ = socket.send_to(&buf[..n], src).await;
            }
        });

        let client_bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: client_bind,
            peer_addr: Some(addr),
        };

        let mut transport = create_async_transport(config)
            .expect("factory create async UDP failed");

        transport.write_all(b"factory-async-udp").await.expect("write failed");
        let mut buf = [0; 64];
        let n = transport.read(&mut buf).await.expect("read failed");
        assert_eq!(&buf[..n], b"factory-async-udp");

        transport.close().await.expect("close failed");
    }
}

// ============================================================
// AsyncTransportConfig derive 验证
// ============================================================

#[cfg(feature = "async")]
mod async_config_derive_tests {
    use super::*;

    #[test]
    fn FunEvent_async_transport_config_clone_works() {
        let addr: SocketAddr = "192.168.1.1:8080".parse().unwrap();
        let original = AsyncTransportConfig::TcpClient { addr };
        let cloned = original.clone();
        match (&original, &cloned) {
            (
                AsyncTransportConfig::TcpClient { addr: a },
                AsyncTransportConfig::TcpClient { addr: b },
            ) => {
                assert_eq!(a, b);
            }
            _ => panic!("clone should preserve variant"),
        }
    }

    #[test]
    fn FunEvent_async_transport_config_debug_format() {
        let addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();
        let config = AsyncTransportConfig::Udp {
            bind_addr: addr,
            peer_addr: None,
        };
        let debug_str = format!("{:?}", config);
        assert!(
            debug_str.contains("Udp") || debug_str.contains("udp"),
            "Debug output should contain type info, got: {}",
            debug_str
        );
    }
}
