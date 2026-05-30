# connect-io

[![Rust](https://img.shields.io/badge/Rust-2024-orange?logo=rust)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![crates.io](https://img.shields.io/crates/v/connect-io.svg)](https://crates.io/crates/connect-io)

> **统一同步/异步传输抽象库** — TCP · UDP · Serial · Tauri 集成

`connect-io` 是一个 Rust 通信库，提供 **同步/异步** 统一的传输抽象层，支持 **TCP、UDP、串口（Serial）** 三种协议，并内置 **Tauri 集成层**。通过统一的 `Transport` / `AsyncTransport` Trait，开发者可以在不同协议间无缝切换，无需修改业务逻辑代码。

---

## ✨ 特性

- 🔄 **双模式 API**：同步（std）+ 异步（tokio），同一套语义
- 🔌 **三协议支持**：TCP 客户端/服务端、UDP（含无连接模式）、串口
- 🏭 **多连接管理**：`TcpServerManager` / `AsyncTcpServerManager` 支持循环 accept
- 📡 **UDP 无连接模式**：`send_to()` / `recv_from()` 支持任意地址收发
- 🎯 **连接状态追踪**：`ConnectionState` 枚举（Connected / Disconnected / Connecting / Unknown）
- 🦀 **Tauri 原生集成**：内置 7 个 Command + 事件推送 + 会话管理（`tauri` feature）
- ⚡ **按需编译**：5 个 Cargo feature，最小化依赖体积
- 🧪 **65+ 测试用例**：单元测试 + 集成测试全覆盖
- 📖 **完整文档注释**：所有公开 API 均有中文文档

---

## 🚀 快速开始

### 安装

```toml
# Cargo.toml — 默认启用全部功能
[dependencies]
connect-io = "0.1.3"

# 最小化：仅 TCP + 异步
[dependencies.connect-io]
version = "0.1.3"
default-features = false
features = ["tcp", "async"]

# Tauri 应用完整功能
[dependencies.connect-io]
version = "0.1.3"
features = ["tcp", "udp", "serial", "async", "tauri"]
```

### Feature 矩阵

| Feature | 功能 | 额外依赖 | 说明 |
|---------|------|----------|------|
| `tcp` | TCP 传输（同步+异步+ServerManager） | tokio/net | 默认启用 |
| `udp` | UDP 传输（同步+异步+无连接模式） | tokio/net | 默认启用 |
| `serial` | 串口传输（同步+异步） | serialport, tokio-serial | 默认启用 |
| `async` | 异步运行时支持 (tokio) | tokio, async-trait | 启用 async_impl 模块 |
| `tauri` | Tauri 2 集成层 | tauri 2, serde, serde_json | 自动启用 async |

---

## 📐 核心架构

```
                    connect-io v0.1.3
    ┌──────────────────────────────────────────┐
    │              公开 API 层                  │
    │  TransportConfig / AsyncTransportConfig   │
    │  Transport trait / AsyncTransport trait   │
    │  ConnectionState 枚举                     │
    │  create_transport() / create_async_transport()
    ├──────────────┬───────────────────────────┤
    │  同步层      │  异步层 (async feature)    │
    │ ┌────────┐  │  ┌──────────────────────┐  │
    │ │TcpTrans│  │  │ AsyncTcpTransport    │  │
    │ │UdpTrans│  │  │ AsyncUdpTransport    │  │
    │ │SerTrans│  │  │ AsyncSerialTransport │  │
    │ └────────┘  │  └──────────────────────┘  │
    │ ┌────────────┐│  ┌──────────────────────┐  │
    │ │TcpServerMgr││  │AsyncTcpServerManager │  │
    │ └────────────┘│  └──────────────────────┘  │
    ├──────────────┴───────────────────────────┤
    │         Tauri 集成层 (tauri feature)     │
    │  TransportState / 7 Commands / Events     │
    └──────────────────────────────────────────┘
```

### 核心类型

#### ConnectionState — 连接状态枚举

```rust
pub enum ConnectionState {
    Connected,       // 已建立连接
    Disconnected,    // 已断开
    Connecting,      // 正在连接中
    Unknown,         // 状态未知（默认值）
}
```

#### Transport Trait（同步）

```rust
pub trait Transport: Read + Write {
    fn connect(config: TransportConfig) -> Result<Self, TransportError>;
    fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn peer_addr(&self) -> Option<SocketAddr>;
    fn connection_state(&self) -> ConnectionState;  // v0.1.3 新增
}
```

#### AsyncTransport Trait（异步）

```rust
#[async_trait]
pub trait AsyncTransport: AsyncRead + AsyncWrite + Send + Sync {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError>;
    async fn close(&mut self) -> Result<(), TransportError>;
    // ... 与同步版本对应的方法
    fn connection_state(&self) -> ConnectionState;  // v0.1.3 新增
}
```

#### TransportError

```rust
pub enum TransportError {
    Io(std::io::Error),              // I/O 错误
    #[cfg(feature = "serial")]
    Serial(serialport::Error),       // 串口错误
    Config(String),                 // 配置错误
    ConnectionFailed(String),       // 连接失败
}
```

---

## 📖 使用指南

### 同步 API

#### TCP 客户端

```rust
use connect_io::{TransportConfig, TcpTransport};
use std::io::{Read, Write};

let addr: SocketAddr = "127.0.0.1:8080".parse()?;
let mut transport = TcpTransport::connect(TransportConfig::TcpClient { addr })?;

transport.write_all(b"hello")?;
let mut buf = [0u8; 1024];
let n = transport.read(&mut buf)?;
println!("Received: {:?}", &buf[..n]);

transport.close()?;
```

#### TCP ServerManager（多连接）— v0.1.3 新增

```rust
use connect_io::TcpServerManager;

let addr: SocketAddr = "0.0.0.0:8080".parse()?;
let server = TcpServerManager::bind(addr)?;

loop {
    match server.accept()? {
        mut client => {
            println!("新客户端: {:?}", client.peer_addr());
            // 处理通信...
        }
    }
}

server.shutdown()?;
```

#### UDP 无连接模式 — v0.1.3 新增

```rust
use connect_io::{TransportConfig, UdpTransport};

// 服务端：绑定端口，不指定 peer
let server = UdpTransport::connect(TransportConfig::Udp {
    bind_addr: "0.0.0.0:9090".parse()?,
    peer_addr: None,
})?;

let mut buf = [0u8; 1024];
loop {
    // 使用 recv_from 接收任意来源的数据
    let (n, from) = server.recv_from(&mut buf)?;
    println!("来自 {:?}: {:?}", from, &buf[..n]);

    // 使用 send_to 回复到指定地址
    server.send_to(&buf[..n], from)?;
}
```

#### 工厂函数（运行时切换协议）

```rust
use connect_io::{create_transport, TransportConfig};

// 通过配置动态选择协议
let config = TransportConfig::TcpClient { addr: "127.0.0.1:8080".parse()? };
let mut transport = create_transport(config)?;

transport.write_all(b"hello")?;
transport.close()?;
```

### 异步 API（需 `async` feature）

#### 异步 TCP + 多连接服务端

```rust
use connect_io::async_impl::{AsyncTcpServerManager, AsyncTransportConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:8080".parse()?;
    let server = AsyncTcpServerManager::bind(addr).await?;

    loop {
        let mut client = server.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = client.read(&mut buf).await {
                if n == 0 { break; }
                client.write_all(&buf[..n]).await.ok();
            }
        });
    }
}
```

#### 异步工厂函数

```rust
use connect_io::async_impl::{create_async_transport, AsyncTransportConfig};

let config = AsyncTransportConfig::TcpClient { addr: "127.0.0.1:8080".parse()? };
let transport = create_async_transport(config).await?;
transport.write_all(b"hello").await?;
```

### Tauri 集成层（需 `tauri` feature）

#### 初始化

```rust
use connect_io::tauri_plugin::TransportState;

tauri::Builder::default()
    .setup(|app| {
        let state = TransportState::new();
        app.manage(state);
        connect_io::tauri_plugin::register_commands(app)?;
        Ok(())
    })
    .run(tauri::generate_context!())?;
```

#### 可用命令

| 命令 | 功能 |
|------|------|
| `transport_connect` | 创建会话并建立连接 |
| `transport_disconnect` | 断开并释放会话 |
| `transport_write` | 写入二进制数据 |
| `transport_read` | 非阻塞轮询读取 |
| `transport_send_to` | UDP 无连接发送 |
| `transport_get_state` | 查询连接状态 |
| `transport_list_sessions` | 列出所有活跃会话 |

#### 前端调用示例

```typescript
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

// 连接
await invoke('transport_connect', {
  sessionId: 'device-1',
  config: { type: 'TcpClient', addr: '192.168.1.100:502' },
});

// 写入
await invoke('transport_write', { sessionId: 'device-1', data: [0x01, 0x02, 0x03] });

// 监听数据事件
const unlisten = await listen('transport://data/device-1', (event) => {
  console.log('收到数据:', event.payload);  // Uint8Array
});
```

---

## 📊 协议对比

| 协议 | 连接方式 | 数据可靠性 | 适用场景 | 多连接支持 |
|------|----------|------------|----------|-----------|
| TCP | 面向连接 | 可靠，有序 | 文件传输、设备控制、Modbus TCP | ✅ ServerManager |
| UDP | 无连接 / 已连接 | 不可靠，快速 | 实时数据、广播、DNS | ✅ send_to/recv_from |
| Serial | 点对点 | 可靠 | 传感器、PLC、工业设备 | N/A |

---

## 🧪 测试

```bash
# 全量测试（含异步）
cargo test --features tcp,udp,serial,async

# 仅同步测试
cargo test

# 仅单元内联测试
cargo test --features async --lib

# clippy 静态检查
cargo clippy --features tcp,udp,serial,async,tauri -- -D warnings
```

### 覆盖率

| 类别 | 用例数 | 文件 |
|------|--------|------|
| 同步集成测试 | 20 | `tests/sync_transport_tests.rs` |
| 异步集成测试 | 18 | `tests/async_transport_tests.rs` |
| 单元内联测试 | 26 | `src/lib.rs`, `src/error.rs` |
| 原有回归测试 | 1 | `tests/integration_test.rs` |
| **总计** | **65+** | |

---

## ⚙️ 注意事项

1. **TCP 单次 accept**：`TransportConfig::TcpServer` 只 accept 一次连接。多客户端请使用 `TcpServerManager`
2. **UDP 未连接模式**：未指定 `peer_addr` 时，`read()`/`write()` 返回 `NotConnected`，应使用 `send_to()`/`recv_from()`
3. **异步超时**：tokio 版本 `set_timeout()` 为空操作，建议使用 `tokio::time::timeout()` 包装
4. **串口路径**：macOS/Linux 为 `/dev/ttyUSB0` 或 `/dev/cu.usbserial-*`，Windows 为 `COM1` 等
5. **Tauri 事件格式**：
   - 数据到达：`transport://data/{session_id}` → 二进制 `Uint8Array`
   - 状态变更：`transport://{session_id}` → JSON `{ connected, state }`
6. **性能约束**（Tauri 集成层）：后台读取间隔 ≤5ms，持有锁时禁止发送 Tauri 事件

---

## 📁 项目结构

```
connect-io/
├── src/
│   ├── lib.rs                      # 入口：Trait 定义 + 工厂函数 + ConnectionState
│   ├── error.rs                    # TransportError 错误类型
│   ├── tcp.rs                      # 同步 TcpTransport
│   ├── tcp_server.rs               # 同步 TcpServerManager（多连接）
│   ├── udp.rs                      # 同步 UdpTransport（含 send_to/recv_from）
│   ├── serial.rs                   # 同步 SerialTransport
│   └── async_impl/
│       ├── mod.rs                  # 异步 Trait 定义 + 工厂函数
│       ├── tcp.rs                  # 异步 AsyncTcpTransport
│       ├── tcp_server.rs           # 异步 AsyncTcpServerManager
│       ├── udp.rs                  # 异步 AsyncUdpTransport
│       └── serial.rs               # 异步 AsyncSerialTransport
├── src/tauri_plugin/               # Tauri 2 集成层（tauri feature）
│   ├── mod.rs                      # 注册入口
│   ├── state.rs                    # TransportState 会话管理器
│   └── commands.rs                 # 7 个 Tauri Command
├── tests/
│   ├── integration_test.rs         # 原始回归测试
│   ├── sync_transport_tests.rs     # 同步全协议测试
│   └── async_transport_tests.rs    # 异步全协议测试
├── examples/                       # 6 个使用示例
└── Cargo.toml                       # Feature 配置
```

---

## 📄 License

[MIT](LICENSE) © Easion Wang

## 🔗 项目地址

- **GitHub**: https://github.com/EasionWang/connect-io
- **crates.io**: https://crates.io/crates/connect-io
- **文档**: 本 README 即为完整使用文档
