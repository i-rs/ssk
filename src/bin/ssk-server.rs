use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("配置解析错误: {0}")]
    Config(#[from] toml::de::Error),
    #[error("UTF-8解码错误: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("连接意外关闭")]
    ConnectionClosed,
    #[error("文件传输中断: {0}%完成")]
    TransferInterrupted(u8),
}

#[derive(Deserialize)]
struct Config {
    server: ServerConfig,
}

#[derive(Deserialize)]
struct ServerConfig {
    address: String,
    upload_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志系统
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("ssk=info".parse().unwrap()))
        .init();

    // 获取命令行参数
    let args: Vec<String> = std::env::args().collect();
    let config_path = args.get(1).map(|s| s.as_str()).unwrap_or("config.toml");

    // 读取配置文件
    info!("正在读取配置文件: {}", config_path);
    let config_content = fs::read_to_string(config_path)?;
    let config: Config = toml::from_str(&config_content)?;

    let addr = &config.server.address;
    let listener = TcpListener::bind(addr).await?;
    info!("服务器正在监听: {}", addr);

    // 确保上传目录存在
    info!("确保上传目录存在: {}", config.server.upload_dir);
    fs::create_dir_all(&config.server.upload_dir)?;

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("新的客户端连接: {}", addr);

        let upload_dir = config.server.upload_dir.clone();
        // 为每个连接创建一个新的任务
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr, &upload_dir).await {
                error!("处理客户端 {} 时出错: {}", addr, e);
            }
        });
    }
}

async fn handle_client(
    mut socket: TcpStream,
    addr: SocketAddr,
    upload_dir: &str,
) -> Result<()> {
    let mut filename_len = [0u8; 8];
    socket.read_exact(&mut filename_len).await?;
    let filename_len = u64::from_be_bytes(filename_len);

    let mut filename = vec![0u8; filename_len as usize];
    socket.read_exact(&mut filename).await?;
    let filename = String::from_utf8(filename)?;

    let mut filesize = [0u8; 8];
    socket.read_exact(&mut filesize).await?;
    let filesize = u64::from_be_bytes(filesize);

    info!(
        "接收来自 {} 的文件: {} ({} bytes)",
        addr, filename, filesize
    );

    // 创建目标文件
    let filepath = Path::new(upload_dir).join(&filename);
    let mut file = tokio::fs::File::create(&filepath).await?;

    let mut received = 0;
    let mut buffer = vec![0u8; 1024 * 64];
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 3;

    while received < filesize {
        let read_result = socket.read(&mut buffer).await;
        match read_result {
            Ok(0) => {
                // 连接关闭，清理未完成的文件
                if let Err(e) = tokio::fs::remove_file(&filepath).await {
                    warn!("删除未完成的文件失败: {}", e);
                }
                return Err(ServerError::ConnectionClosed.into());
            }
            Ok(n) => {
                file.write_all(&buffer[..n]).await?;
                received += n as u64;
                retry_count = 0; // 成功读取后重置重试计数

                // 发送进度确认
                let progress = (received as f64 / filesize as f64 * 100.0) as u8;
                if let Err(e) = socket.write_all(&[progress]).await {
                    warn!("发送进度确认失败: {}", e);
                }
            }
            Err(e) => {
                warn!("读取数据时出错: {}, 重试次数: {}", e, retry_count);
                if retry_count >= MAX_RETRIES {
                    // 超过最大重试次数，清理文件并返回错误
                    if let Err(e) = tokio::fs::remove_file(&filepath).await {
                        warn!("删除未完成的文件失败: {}", e);
                    }
                    let progress = (received as f64 / filesize as f64 * 100.0) as u8;
                    return Err(ServerError::TransferInterrupted(progress).into());
                }
                retry_count += 1;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
        }
    }

    info!("文件 {} 接收完成", filename);
    Ok(())
}
