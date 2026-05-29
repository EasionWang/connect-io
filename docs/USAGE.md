# connect-io 对接文档

## 项目概述

`connect-io` 是一个 Rust 通信库，提供 **同步/异步** 统一的传输抽象层，支持 **TCP、UDP、串口（Serial）** 三种协议。通过统一的 `Transport` / `AsyncTransport` Trait，开发者可以在不同协议间无缝切换，无需修改业务逻辑代码。

### 特性

- 统一的同步/异步 API 接口
- 支持 TCP 客户端/服务端、UDP、串口
- 基于 Cargo feature 按需编译，减少依赖体积
- 兼容 tokio 异步运行时
- 零额外抽象开销

### 适用场景

- Tauri 应用中的设备通信
- 工业协议网关（Modbus TCP/RTU）
- 嵌入式设备数据采集
- 多协议统一抽象层

---

## 快速开始

### 安装

```toml
[dependencies]
connect-io = "0.1.1"
```

### Cargo.toml 特性配置

```toml
# 默认启用所有功能
[dependencies]
connect-io = "0.1.1"

# 仅启用 TCP + 异步
[dependencies.connect-io]
version = "0.1.1"
default-features = false
features = ["tcp", "async"]
```

### 特性组合表

| Feature | 功能 | 额外依赖 |
|---------|------|----------|
| `tcp` | TCP 传输（同步+异步） | tokio/net |
| `udp` | UDP 传输（同步+异步） | tokio/net |
| `serial` | 串口传输（同步+异步） | serialport, tokio-serial |
| `async` | 异步运行时支持 | tokio, async-trait |

---

## 核心概念

### TransportConfig（同步配置）

```rust
pub enum TransportConfig {
    TcpClient { addr: SocketAddr },
    TcpServer { bind_addr: SocketAddr },
    Udp {
        bind_addr: SocketAddr,
        peer_addr: Option<SocketAddr>,
    },
    Serial {
        port: String,
        baud_rate: u32,
        data_bits: serialport::DataBits,
        stop_bits: serialport::StopBits,
        parity: serialport::Parity,
        flow_control: serialport::FlowControl,
    },
}
```

### AsyncTransportConfig（异步配置）

与 `TransportConfig` 结构相同，位于 `connect_io::async_impl::AsyncTransportConfig`。

### Transport Trait（同步）

```rust
pub trait Transport: Read + Write {
    fn connect(config: TransportConfig) -> Result<Self, TransportError>;
    fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn peer_addr(&self) -> Option<SocketAddr>;
}
```

### AsyncTransport Trait（异步）

```rust
#[async_trait]
pub trait AsyncTransport: AsyncRead + AsyncWrite + Send + Sync {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError>;
    async fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn peer_addr(&self) -> Option<SocketAddr>;
}
```

### TransportError

```rust
pub enum TransportError {
    Io(std::io::Error),
    Serial(serialport::Error),  // serial feature
    Config(String),
    ConnectionFailed(String),
}
```

---

## 同步 API 使用指南

### TCP 客户端

```rust
use connect_io::{TransportConfig, TransportError};
use connect_io::tcp::TcpTransport;
use std::io::{Read, Write};

fn main() -> Result<(), TransportError> {
    let addr = "127.0.0.1:8080".parse().unwrap();
    let config = TransportConfig::TcpClient { addr };
    
    let mut transport = TcpTransport::connect(config)?;
    
    transport.write_all(b"hello")?;
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf)?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close()?;
    Ok(())
}
```

### TCP 服务端

```rust
use connect_io::{TransportConfig, TransportError};
use connect_io::tcp::TcpTransport;
use std::io::{Read, Write};

fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:8080".parse().unwrap();
    let config = TransportConfig::TcpServer { bind_addr };
    
    let mut transport = TcpTransport::connect(config)?;
    println!("Client connected!");
    
    let mut buf = [0u8; 1024];
    loop {
        let n = transport.read(&mut buf)?;
        if n == 0 { break; }
        transport.write_all(&buf[..n])?;
    }
    
    transport.close()?;
    Ok(())
}
```

### UDP 客户端

```rust
use connect_io::{TransportConfig, TransportError};
use connect_io::udp::UdpTransport;
use std::io::{Read, Write};

fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let peer_addr = "127.0.0.1:9090".parse().unwrap();
    let config = TransportConfig::Udp {
        bind_addr,
        peer_addr: Some(peer_addr),
    };
    
    let mut transport = UdpTransport::connect(config)?;
    transport.write_all(b"hello")?;
    
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf)?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close()?;
    Ok(())
}
```

### UDP 服务端

```rust
use connect_io::{TransportConfig, TransportError};
use connect_io::udp::UdpTransport;
use std::io::{Read, Write};

fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:9090".parse().unwrap();
    let config = TransportConfig::Udp {
        bind_addr,
        peer_addr: None,
    };
    
    let mut transport = UdpTransport::connect(config)?;
    let mut buf = [0u8; 1024];
    
    loop {
        match transport.read(&mut buf) {
            Ok(n) if n > 0 => {
                transport.write_all(&buf[..n])?;
            }
            Err(e) => eprintln!("Error: {}", e),
            _ => {}
        }
    }
}
```

### 串口

```rust
use connect_io::{TransportConfig, TransportError};
use connect_io::serial::SerialTransport;
use serialport::{DataBits, StopBits, Parity, FlowControl};
use std::io::{Read, Write};

fn main() -> Result<(), TransportError> {
    let config = TransportConfig::Serial {
        port: "/dev/ttyUSB0".to_string(),
        baud_rate: 115200,
        data_bits: DataBits::Eight,
        stop_bits: StopBits::One,
        parity: Parity::None,
        flow_control: FlowControl::None,
    };
    
    let mut transport = SerialTransport::connect(config)?;
    transport.write_all(b"hello")?;
    
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf)?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close()?;
    Ok(())
}
```

---

## 异步 API 使用指南

> 需要启用 `async` feature

### TCP 客户端

```rust
use connect_io::async_impl::{AsyncTransportConfig, AsyncTcpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let addr = "127.0.0.1:8080".parse().unwrap();
    let config = AsyncTransportConfig::TcpClient { addr };
    
    let mut transport = AsyncTcpTransport::connect(config).await?;
    transport.write_all(b"hello").await?;
    
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf).await?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close().await?;
    Ok(())
}
```

### TCP 服务端

```rust
use connect_io::async_impl::{AsyncTransportConfig, AsyncTcpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:8080".parse().unwrap();
    let config = AsyncTransportConfig::TcpServer { bind_addr };
    
    let mut transport = AsyncTcpTransport::connect(config).await?;
    println!("Client connected!");
    
    let mut buf = [0u8; 1024];
    loop {
        let n = transport.read(&mut buf).await?;
        if n == 0 { break; }
        transport.write_all(&buf[..n]).await?;
    }
    
    transport.close().await?;
    Ok(())
}
```

### UDP 客户端

```rust
use connect_io::async_impl::{AsyncTransportConfig, AsyncUdpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:0".parse().unwrap();
    let peer_addr = "127.0.0.1:9090".parse().unwrap();
    let config = AsyncTransportConfig::Udp {
        bind_addr,
        peer_addr: Some(peer_addr),
    };
    
    let mut transport = AsyncUdpTransport::connect(config).await?;
    transport.write_all(b"hello").await?;
    
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf).await?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close().await?;
    Ok(())
}
```

### UDP 服务端

```rust
use connect_io::async_impl::{AsyncTransportConfig, AsyncUdpTransport};
use connect_io::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let bind_addr = "127.0.0.1:9090".parse().unwrap();
    let config = AsyncTransportConfig::Udp {
        bind_addr,
        peer_addr: None,
    };
    
    let mut transport = AsyncUdpTransport::connect(config).await?;
    let mut buf = [0u8; 1024];
    
    loop {
        match transport.read(&mut buf).await {
            Ok(n) if n > 0 => {
                transport.write_all(&buf[..n]).await?;
            }
            Err(e) => eprintln!("Error: {}", e),
            _ => {}
        }
    }
}
```

### 串口

```rust
use connect_io::async_impl::{AsyncTransportConfig, AsyncSerialTransport};
use connect_io::TransportError;
use serialport::{DataBits, StopBits, Parity, FlowControl};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let config = AsyncTransportConfig::Serial {
        port: "/dev/ttyUSB0".to_string(),
        baud_rate: 115200,
        data_bits: DataBits::Eight,
        stop_bits: StopBits::One,
        parity: Parity::None,
        flow_control: FlowControl::None,
    };
    
    let mut transport = AsyncSerialTransport::connect(config).await?;
    transport.write_all(b"hello").await?;
    
    let mut buf = [0u8; 1024];
    let n = transport.read(&mut buf).await?;
    println!("Received: {:?}", &buf[..n]);
    
    transport.close().await?;
    Ok(())
}
```

---

## 工厂函数使用

### 同步工厂函数

```rust
use connect_io::{create_transport, TransportConfig};
use std::io::{Read, Write};

let config = TransportConfig::TcpClient {
    addr: "127.0.0.1:8080".parse().unwrap(),
};
let mut transport = create_transport(config)?;
transport.write_all(b"hello")?;
```

### 异步工厂函数

```rust
use connect_io::async_impl::{create_async_transport, AsyncTransportConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

let config = AsyncTransportConfig::TcpClient {
    addr: "127.0.0.1:8080".parse().unwrap(),
};
let mut transport = create_async_transport(config)?;
transport.write_all(b"hello").await?;
```

---

## 协议对比

| 协议 | 连接方式 | 数据可靠性 | 适用场景 |
|------|----------|------------|----------|
| TCP | 面向连接 | 可靠，有序 | 文件传输、设备控制 |
| UDP | 无连接 | 不可靠，快速 | 实时数据、广播 |
| Serial | 点对点 | 可靠 | 传感器、工业设备 |

---

## 注意事项

1. **TCP 服务端**：当前 `TcpServer` 只 accept 一次连接，多客户端需自行实现 accept 循环
2. **UDP 未连接模式**：同步 `UdpTransport` 在未连接状态下 `Read`/`Write` 会返回 `NotConnected` 错误
3. **异步超时**：tokio 使用独立的 `tokio::time::timeout` API，`set_timeout` 为空操作
4. **串口路径**：macOS/Linux 通常为 `/dev/ttyUSB0` 或 `/dev/cu.usbserial-*`，Windows 为 `COM1`、`COM2` 等

---

## 项目地址

- GitHub: https://github.com/EasionWang/connect-io
- crates.io: https://crates.io/crates/connect-io
