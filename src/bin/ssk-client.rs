use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

const BUFFER_SIZE: usize = 1024 * 64; // 64KB buffer

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let args: Vec<String> = std::env::args().collect();
  if args.len() < 3 {
    eprintln!(
      "用法: {} <服务器地址:端口> <文件路径1> [文件路径2 ...]",
      args[0]
    );
    std::process::exit(1);
  }

  let server_addr = &args[1];
  let files: Vec<PathBuf> = args[2..].iter().map(PathBuf::from).collect();

  for file_path in files {
    if let Err(e) = send_file(server_addr, &file_path).await {
      eprintln!("发送文件 {} 时出错: {}", file_path.display(), e);
    }
  }

  Ok(())
}

async fn send_file(server_addr: &str, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
  let mut stream = TcpStream::connect(server_addr).await?;
  let file = tokio::fs::File::open(file_path).await?;
  let metadata = file.metadata().await?;
  let filesize = metadata.len();

  // 发送文件名
  let filename = file_path.file_name().unwrap().to_string_lossy();
  let filename_bytes = filename.as_bytes();
  stream
    .write_all(&(filename_bytes.len() as u64).to_be_bytes())
    .await?;
  stream.write_all(filename_bytes).await?;

  // 发送文件大小
  stream.write_all(&filesize.to_be_bytes()).await?;

  // 发送文件内容
  let mut file = tokio::fs::File::open(file_path).await?;
  let mut buffer = vec![0u8; BUFFER_SIZE];
  let mut sent = 0;

  println!("开始发送文件: {}", filename);

  let pb = ProgressBar::new(filesize);
  pb.set_style(
    ProgressStyle::default_bar()
      .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
      .unwrap()
      .progress_chars("=>-"),
  );

  while sent < filesize {
    let n = file.read(&mut buffer).await?;
    if n == 0 {
      break;
    }
    stream.write_all(&buffer[..n]).await?;
    sent += n as u64;

    // 接收进度确认
    let mut progress = [0u8; 1];
    stream.read_exact(&mut progress).await?;
    pb.set_position(sent);
  }

  pb.finish_with_message(format!("文件 {} 发送完成", filename));
  Ok(())
}
