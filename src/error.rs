/*
 * @Author       : Easion_Wang Easion.YX@outlook.com
 * @Date         : 2026-05-29 18:32:31
 * @FilePath     : /connect-io/src/error.rs
 * @LastEditors  : Easion_Wang Easion.YX@outlook.com
 * @LastEditTime : 2026-05-29 18:32:32
 * @Description  : 
 */
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "serial")]
    #[error("Serial port error: {0}")]
    Serial(#[from] serialport::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
}