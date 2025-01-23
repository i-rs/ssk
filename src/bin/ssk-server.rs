use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use std::path::Path;
use std::fs;

const BUFFER_SIZE: usize = 1024 * 64; // 64KB buffer
const UPLOAD_DIR: &str = "uploads";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:8888";
    let listener = TcpListener::bind(addr).await?;
    println!("服务器正在监听: {}", addr);

    // 确保上传目录存在
    fs::create_dir_all(UPLOAD_DIR)?;

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("新的客户端连接: {}", addr);

        // 为每个连接创建一个新的任务
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr).await {
                eprintln!("处理客户端 {} 时出错: {}", addr, e);
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut filename_len = [0u8; 8];
    socket.read_exact(&mut filename_len).await?;
    let filename_len = u64::from_be_bytes(filename_len);

    let mut filename = vec![0u8; filename_len as usize];
    socket.read_exact(&mut filename).await?;
    let filename = String::from_utf8(filename)?;

    let mut filesize = [0u8; 8];
    socket.read_exact(&mut filesize).await?;
    let filesize = u64::from_be_bytes(filesize);

    println!("接收来自 {} 的文件: {} ({} bytes)", addr, filename, filesize);

    // 创建目标文件
    let filepath = Path::new(UPLOAD_DIR).join(&filename);
    let mut file = tokio::fs::File::create(&filepath).await?;

    let mut received = 0;
    let mut buffer = vec![0u8; BUFFER_SIZE];

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