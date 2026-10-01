use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub async fn get(address: SocketAddr, path: &str) -> String {
    request(
        address,
        &format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"),
    )
    .await
}

pub async fn post_json(address: SocketAddr, path: &str, body: &str) -> String {
    request(
        address,
        &format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

pub async fn request(address: SocketAddr, request: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to test listener");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write test request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read test response");
    response
}

pub async fn head(address: SocketAddr, request: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to test listener");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write test request");
    read_head(&mut stream).await
}

pub async fn websocket_text(address: SocketAddr, path: &str, message: &str) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect websocket test client");
    stream
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: keep-alive, Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write websocket upgrade request");
    let head = read_head(&mut stream).await;
    stream
        .write_all(&masked_text(message))
        .await
        .expect("write websocket text frame");
    let response = read_text_frame(&mut stream).await;
    (head, response)
}

pub async fn websocket_open_then_close(address: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect websocket test client");
    stream
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: keep-alive, Upgrade\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write websocket upgrade request");
    let head = read_head(&mut stream).await;
    stream
        .write_all(&masked_close())
        .await
        .expect("write websocket close frame");
    head
}

async fn read_head(stream: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut byte = [0; 1];
    while !bytes.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .await
            .expect("read response head byte");
        bytes.push(byte[0]);
    }
    String::from_utf8(bytes).expect("response head is utf-8")
}

fn masked_text(message: &str) -> Vec<u8> {
    let payload = message.as_bytes();
    assert!(
        payload.len() < 126,
        "test websocket frames support small payloads"
    );
    let mask = [0x11, 0x22, 0x33, 0x44];
    let mut frame = Vec::with_capacity(6 + payload.len());
    frame.push(0x81);
    frame.push(0x80 | u8::try_from(payload.len()).expect("small payload length fits in u8"));
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ mask[index % 4]),
    );
    frame
}

fn masked_close() -> Vec<u8> {
    let mask = [0x55, 0x66, 0x77, 0x88];
    let mut frame = Vec::with_capacity(6);
    frame.push(0x88);
    frame.push(0x80);
    frame.extend_from_slice(&mask);
    frame
}

async fn read_text_frame(stream: &mut tokio::net::TcpStream) -> String {
    let mut head = [0; 2];
    stream
        .read_exact(&mut head)
        .await
        .expect("read websocket frame head");
    assert_eq!(head[0] & 0x0f, 1);
    let masked = head[1] & 0x80 != 0;
    let len = usize::from(head[1] & 0x7f);
    assert!(len < 126, "test websocket frames support small payloads");
    let mask = if masked {
        let mut mask = [0; 4];
        stream
            .read_exact(&mut mask)
            .await
            .expect("read websocket frame mask");
        Some(mask)
    } else {
        None
    };
    let mut payload = vec![0; len];
    stream
        .read_exact(&mut payload)
        .await
        .expect("read websocket frame payload");
    if let Some(mask) = mask {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    String::from_utf8(payload).expect("websocket text frame is utf-8")
}
