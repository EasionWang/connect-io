# connect-io

[![Rust](https://img.shields.io/badge/Rust-2024-orange?logo=rust)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![crates.io](https://img.shields.io/crates/v/connect-io.svg)](https://crates.io/crates/connect-io)

> **统一同步/异步传输抽象库** — TCP · UDP · Serial · Tauri 集成

`connect-io` 是一个 Rust 通信库，提供 **同步/异步** 统一的传输抽象层，支持 **TCP、UDP、串口（Serial）** 三种协议，并内置 **Tauri 集成层**。通过统一的 `Transport` / `AsyncTransport` Trait，开发者可以在不同协议间无缝切换，无需修改业务逻辑代码。

---

## ✨ 特性

- 🔄 **双模式 API**：同步（std）+ 异步（tokio），同一套语义
- 🔌 **三协议支持**：TCP 客户端/服务端、UDP（含广播/组播/无连接模式）、串口
- 🏭 **多连接管理**：`TcpServerManager` / `AsyncTcpServerManager` 支持循环 accept
- 📡 **UDP 广播/组播**：`set_broadcast()`、`join/leave_multicast_v4/v6()`、组播 TTL/回环控制
- 🎯 **连接状态追踪**：`ConnectionState` 枚举（Connected / Disconnected / Connecting / Unknown）
- 🦀 **Tauri 原生集成**：内置 12 个 Command + 事件推送 + 会话管理（`tauri` feature）
- ⚡ **按需编译**：5 个 Cargo feature，最小化依赖体积
- 🧪 **65+ 测试用例**：单元测试 + 集成测试全覆盖
- 📖 **完整文档注释**：所有公开 API 均有中文文档

---

## 🚀 快速开始

### 安装

```toml
# Cargo.toml — 默认启用全部功能
[dependencies]
connect-io = "0.1.5"

# 最小化：仅 TCP + 异步
[dependencies.connect-io]
version = "0.1.5"
default-features = false
features = ["tcp", "async"]

# Tauri 应用完整功能
[dependencies.connect-io]
version = "0.1.5"
features = ["tcp", "udp", "serial", "async", "tauri"]
```

### Feature 矩阵

| Feature | 功能 | 额外依赖 | 说明 |
|---------|------|----------|------|
| `tcp` | TCP 传输（同步+异步+ServerManager） | tokio/net | 默认启用 |
| `udp` | UDP 传输（同步+异步+广播+组播+无连接模式） | tokio/net | 默认启用 |
| `serial` | 串口传输（同步+异步） | serialport, tokio-serial | 默认启用 |
| `async` | 异步运行时支持 (tokio) | tokio, async-trait | 启用 async_impl 模块 |
| `tauri` | Tauri 2 集成层 | tauri 2, serde, serde_json | 自动启用 async |

---

## 📐 核心架构

```
                    connect-io v0.1.5
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
    │  TransportState / 12 Commands / Events   │
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
    fn connection_state(&self) -> ConnectionState;
}
```

#### AsyncTransport Trait（异步）

```rust
#[async_trait]
pub trait AsyncTransport: AsyncRead + AsyncWrite + Send + Sync {
    async fn connect(config: AsyncTransportConfig) -> Result<Self, TransportError>;
    async fn close(&mut self) -> Result<(), TransportError>;
    fn is_connected(&self) -> bool;
    fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<(), TransportError>;
    fn local_addr(&self) -> Option<SocketAddr>;
    fn peer_addr(&self) -> Option<SocketAddr>;
    fn connection_state(&self) -> ConnectionState;
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

#### TCP ServerManager（多连接）

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

#### UDP 无连接模式

```rust
use connect_io::{TransportConfig, UdpTransport};

let server = UdpTransport::connect(TransportConfig::Udp {
    bind_addr: "0.0.0.0:9090".parse()?,
    peer_addr: None,
})?;

let mut buf = [0u8; 1024];
loop {
    let (n, from) = server.recv_from(&mut buf)?;
    println!("来自 {:?}: {:?}", from, &buf[..n]);
    server.send_to(&buf[..n], from)?;
}
```

#### UDP 广播 — v0.1.5 新增

```rust
use connect_io::{TransportConfig, UdpTransport};

let transport = UdpTransport::connect(TransportConfig::Udp {
    bind_addr: "0.0.0.0:9090".parse()?,
    peer_addr: None,
})?;

// 启用广播
transport.set_broadcast(true)?;

// 向广播地址发送
transport.send_to(b"Hello", "255.255.255.255:9090".parse()?)?;
```

#### UDP 组播 — v0.1.5 新增

```rust
use connect_io::{TransportConfig, UdpTransport};
use std::net::Ipv4Addr;

let transport = UdpTransport::connect(TransportConfig::Udp {
    bind_addr: "0.0.0.0:9090".parse()?,
    peer_addr: None,
})?;

// 加入 IPv4 组播组
transport.join_multicast_v4(
    "224.0.0.1".parse()?,
    "0.0.0.0".parse()?,
)?;

// 设置组播 TTL（1 = 仅本地网络）
transport.set_multicast_ttl_v4(1)?;

// 向组播组发送
transport.send_to(b"Hello", "224.0.0.1:9090".parse()?)?;

// 离开组播组
transport.leave_multicast_v4(
    "224.0.0.1".parse()?,
    "0.0.0.0".parse()?,
)?;
```

#### 工厂函数（运行时切换协议）

```rust
use connect_io::{create_transport, TransportConfig};

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

#### 异步 UDP 广播/组播

```rust
use connect_io::async_impl::{AsyncUdpTransport, AsyncTransportConfig};

let transport = AsyncUdpTransport::connect(config).await?;

// 广播
transport.set_broadcast(true)?;
transport.send_to(b"Hello", "255.255.255.255:9090".parse()?).await?;

// 组播
transport.join_multicast_v4("224.0.0.1".parse()?, "0.0.0.0".parse()?)?;
transport.send_to(b"Hello", "224.0.0.1:9090".parse()?).await?;
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
        Ok(())
    })
    .invoke_handler(connect_io::tauri_plugin::invoke_handler())
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
| `transport_set_broadcast` | 设置 UDP 广播模式 |
| `transport_join_multicast_v4` | 加入 IPv4 组播组 |
| `transport_leave_multicast_v4` | 离开 IPv4 组播组 |
| `transport_join_multicast_v6` | 加入 IPv6 组播组 |
| `transport_leave_multicast_v6` | 离开 IPv6 组播组 |

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

// UDP 广播
await invoke('transport_set_broadcast', { sessionId: 'udp-1', flag: true });
await invoke('transport_send_to', {
  sessionId: 'udp-1',
  data: [0x01, 0x02],
  addr: '255.255.255.255:9090',
});

// UDP 组播
await invoke('transport_join_multicast_v4', {
  sessionId: 'udp-1',
  multiaddr: '224.0.0.1',
  iface: '0.0.0.0',
});

// 监听数据事件
const unlisten = await listen('transport://data/device-1', (event) => {
  console.log('收到数据:', event.payload);
});
```

---

## 📊 协议对比

| 协议 | 连接方式 | 数据可靠性 | 适用场景 | 多连接支持 | 广播/组播 |
|------|----------|------------|----------|-----------|----------|
| TCP | 面向连接 | 可靠，有序 | 文件传输、设备控制、Modbus TCP | ✅ ServerManager | ❌ |
| UDP | 无连接 / 已连接 | 不可靠，快速 | 实时数据、广播、组播、DNS | ✅ send_to/recv_from | ✅ 广播 + 组播 |
| Serial | 点对点 | 可靠 | 传感器、PLC、工业设备 | N/A | ❌ |

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
| 单元内联测试 | 22 | `src/lib.rs`, `src/error.rs` |
| 原有回归测试 | 1 | `tests/integration_test.rs` |
| **总计** | **61+** | |

---

## ⚙️ 注意事项

1. **TCP 单次 accept**：`TransportConfig::TcpServer` 只 accept 一次连接。多客户端请使用 `TcpServerManager`
2. **UDP 未连接模式**：未指定 `peer_addr` 时，`read()`/`write()` 返回 `NotConnected`，应使用 `send_to()`/`recv_from()`
3. **UDP 广播**：发送广播前需调用 `set_broadcast(true)`，否则系统会拒绝发送到广播地址
4. **UDP 组播**：`join_multicast_v4` 的 `iface` 参数使用 `0.0.0.0` 表示任意接口；IPv6 使用接口索引（0 = 默认接口）
5. **异步超时**：tokio 版本 `set_timeout()` 为空操作，建议使用 `tokio::time::timeout()` 包装
6. **串口路径**：macOS/Linux 为 `/dev/ttyUSB0` 或 `/dev/cu.usbserial-*`，Windows 为 `COM1` 等
7. **串口流控**：serialport 4.x 仅支持 `FlowControl::None/Hardware/Software`，不支持 Mark/Space 校验
8. **Tauri 事件格式**：
   - 数据到达：`transport://data/{session_id}` → 二进制 `Uint8Array`
   - 状态变更：`transport://{session_id}` → JSON `{ connected, state }`
9. **性能约束**（Tauri 集成层）：后台读取间隔 ≤5ms，持有锁时禁止发送 Tauri 事件

---

## 📁 项目结构

```
connect-io/
├── src/
│   ├── lib.rs                      # 入口：Trait 定义 + 工厂函数 + ConnectionState
│   ├── error.rs                    # TransportError 错误类型
│   ├── tcp.rs                      # 同步 TcpTransport
│   ├── tcp_server.rs               # 同步 TcpServerManager（多连接）
│   ├── udp.rs                      # 同步 UdpTransport（含广播/组播/send_to/recv_from）
│   ├── serial.rs                   # 同步 SerialTransport
│   └── async_impl/
│       ├── mod.rs                  # 异步 Trait 定义 + 工厂函数
│       ├── tcp.rs                  # 异步 AsyncTcpTransport
│       ├── tcp_server.rs           # 异步 AsyncTcpServerManager
│       ├── udp.rs                  # 异步 AsyncUdpTransport（含广播/组播）
│       └── serial.rs               # 异步 AsyncSerialTransport
├── src/tauri_plugin/               # Tauri 2 集成层（tauri feature）
│   ├── mod.rs                      # invoke_handler 注册入口
│   ├── state.rs                    # TransportState 会话管理器
│   └── commands.rs                 # 12 个 Tauri Command
├── tests/
│   ├── integration_test.rs         # 原始回归测试
│   ├── sync_transport_tests.rs     # 同步全协议测试
│   └── async_transport_tests.rs    # 异步全协议测试
├── examples/                       # 使用示例
└── Cargo.toml                       # Feature 配置
```

---

## 📝 更新日志

### v0.1.5

- **新增** UDP 广播支持：`set_broadcast()`
- **新增** UDP IPv4 组播：`join_multicast_v4()`、`leave_multicast_v4()`、`set_multicast_loop_v4()`、`set_multicast_ttl_v4()`
- **新增** UDP IPv6 组播：`join_multicast_v6()`、`leave_multicast_v6()`、`set_multicast_loop_v6()`
- **新增** Tauri 集成层 5 个组播/广播命令
- **修复** `create_async_transport` 改为 `async fn`
- **修复** TCP `close()` 缺少 `AsyncWriteExt` 导入及 `shutdown().await`
- **修复** UDP `poll_read` 使用 `ReadBuf` 替代 `Vec<u8>`
- **修复** Tauri 命令注册改为 `invoke_handler()` 函数
- **修复** serialport 枚举变体兼容性（`FlowControl::Hardware/Software`）

---

## 📄 License

[MIT](LICENSE) © Easion Wang

## 🔗 项目地址

- **GitHub**: https://github.com/EasionWang/connect-io
- **crates.io**: https://crates.io/crates/connect-io
- **文档**: 本 README 即为完整使用文档
