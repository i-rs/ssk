use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
};

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 读取配置文件
  let config_content = fs::read_to_string("config.toml")?;
  let config: Config = toml::from_str(&config_content)?;

  let addr = &config.server.address;
  let listener = TcpListener::bind(addr).await?;
  println!("服务器正在监听: {}", addr);

  // 确保上传目录存在
  fs::create_dir_all(&config.server.upload_dir)?;

  loop {
    let (socket, addr) = listener.accept().await?;
    println!("新的客户端连接: {}", addr);

    let upload_dir = config.server.upload_dir.clone();
    // 为每个连接创建一个新的任务
    tokio::spawn(async move {
      if let Err(e) = handle_client(socket, addr, &upload_dir).await {
        eprintln!("处理客户端 {} 时出错: {}", addr, e);
      }
    });
  }
}

async fn handle_client(
  mut socket: TcpStream,
  addr: SocketAddr,
  upload_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
  let mut filename_len = [0u8; 8];
  socket.read_exact(&mut filename_len).await?;
  let filename_len = u64::from_be_bytes(filename_len);

  let mut filename = vec![0u8; filename_len as usize];
  socket.read_exact(&mut filename).await?;
  let filename = String::from_utf8(filename)?;

  let mut filesize = [0u8; 8];
  socket.read_exact(&mut filesize).await?;
  let filesize = u64::from_be_bytes(filesize);

  println!(
    "接收来自 {} 的文件: {} ({} bytes)",
    addr, filename, filesize
  );

  // 创建目标文件
  let filepath = Path::new(upload_dir).join(&filename);
  let mut file = tokio::fs::File::create(&filepath).await?;

  let mut received = 0;
  let mut buffer = vec![0u8; 1024 * 64];

  while received < filesize {
    let n = socket.read(&mut buffer).await?;
    if n == 0 {
      return Err("连接意外关闭".into());
    }
    file.write_all(&buffer[..n]).await?;
    received += n as u64;

    // 发送进度确认
    let progress = (received as f64 / filesize as f64 * 100.0) as u8;
    socket.write_all(&[progress]).await?;
  }

  println!("文件 {} 接收完成", filename);
  Ok(())
}
