use anyhow::{Result, Context};

use std::sync::{Arc, Mutex};

use tracing::{trace, debug};

use lazy_static::lazy_static;

use tokio::process::Command;

use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::protocol::Message};

use serde_json::json;
use serde::Deserialize;
use serde_tuple::Deserialize_tuple;


use futures::SinkExt;
use futures_util::StreamExt;

use rand::distributions::{Alphanumeric, DistString};
use sha2::{Sha512, Digest};

use tokio::io::AsyncWriteExt;

const TOKEN_LENGTH: usize = 50;

fn sha512hex(text: &str) -> String {
    let hash = Sha512::digest(text);
    format!("{:x}", hash)
}

fn generate_token() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), TOKEN_LENGTH)
}

#[derive(Deserialize_tuple)]
struct Htmlblock {
    hash: u64,
    html: String,
}

#[derive(Deserialize)]
struct NewContentMessage {
    filepath: String,
    htmlblocks: Vec<Htmlblock>,
    bibid: Option<u64>,
    #[serde(rename = "suppress-bibliography")]
    suppress_bibliography: bool,
    toc: bool,
    #[serde(rename = "toc-title")]
    toc_title: Option<String>,
    #[serde(rename = "reference-section-title")]
    reference_section_title: String,
}

// Python has i64, not u64 hash, so we need different structs

#[derive(Deserialize_tuple)]
struct PyHtmlblock {
    hash: i64,
    html: String,
}

#[derive(Deserialize)]
struct PyNewContentMessage {
    filepath: String,
    htmlblocks: Vec<PyHtmlblock>,
    bibid: Option<i64>,
    #[serde(rename = "suppress-bibliography")]
    suppress_bibliography: bool,
    toc: bool,
    #[serde(rename = "toc-title")]
    toc_title: Option<String>,
    #[serde(rename = "reference-section-title")]
    reference_section_title: String,
}

async fn do_auth(ws_stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, secret: &str) -> Result<()> {
    let msg = ws_stream.next().await.context("no websocket msg")??;
    let msg: serde_json::Value = serde_json::from_str(msg.to_text()?)?;
    let cnonce = generate_token();
    let challenge = generate_token();
    let msg = Message::text(serde_json::to_string(&json!({
        "hash": sha512hex(format!("{}{}{}", secret, msg["challenge"].as_str().context("no str")?, cnonce).as_str()),
        "challenge": challenge,
        "cnonce": cnonce,
    }))?);
    ws_stream.send(msg).await?;

    let msg = ws_stream.next().await.context("no websocket msg")??;
    let msg: serde_json::Value = serde_json::from_str(msg.to_text()?)?;

    assert_eq!(
        sha512hex(format!("{}{}{}", secret, challenge, msg["snonce"].as_str().context("no str")?).as_str()),
        msg["hash"].as_str().context("no str")?);

    Ok(())
}

static INIT_LOG: std::sync::Once = std::sync::Once::new();

lazy_static! {
    static ref NEXT_PORT: Arc<Mutex<u16>> = Arc::new(Mutex::new(9900));
}

struct TestServer {
    // Unused but needed here because we use `kill_on_drop`
    _child: tokio::process::Child,
    port: u16,
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    runtime_dir: tempfile::TempDir,
    is_python: bool,
}

impl TestServer {

    fn new_port() -> u16 {
        let mut next_port = NEXT_PORT.lock().unwrap();
        let port = *next_port;
        *next_port += 1;
        port
    }

    async fn new_from_command(mut cmd: Command, is_python: bool) -> Result<TestServer> {
        INIT_LOG.call_once(|| {
            tracing_subscriber::fmt::init();
        });

        let port = Self::new_port();

        let runtime_dir = tempfile::tempdir()?;

        cmd
            .env("XDG_RUNTIME_DIR", runtime_dir.path())
            .kill_on_drop(true)
            .arg("--port").arg(port.to_string());
        let child = cmd.spawn()?;

        let mut secret = None;
        while secret.is_none() {
            if let Ok(content) = std::fs::read_to_string(runtime_dir.path().join("pmpm/websocket_secret")) {
                secret = Some(content);
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        let secret = secret.context("no secret")?;

        let (mut ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/", port))
            .await.expect("Failed to connect");
        do_auth(&mut ws_stream, &secret).await?;

        Ok(TestServer {
            _child: child,
            port: port,
            runtime_dir: runtime_dir,
            ws_stream: ws_stream,
            is_python: is_python,
        })
    }

    async fn new(use_release: bool) -> Result<TestServer> {
        let mut cmd = Command::new("cargo");
        cmd.arg("run");
        if use_release {
            cmd.arg("--release");
        }
        #[cfg(feature = "cmark")]
        cmd.arg("--features").arg("cmark");
        cmd.arg("--");

        Self::new_from_command(cmd, false).await
    }

    async fn new_python() -> Result<TestServer> {
        let mut cmd = Command::new("pmpm-websocket");
        cmd.arg("--math").arg("katex");

        Self::new_from_command(cmd, true).await
    }


    async fn open_pipe(&self) -> tokio::fs::File {
        tokio::fs::OpenOptions::new()
            .read(false)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(self.runtime_dir.path().join("pmpm/pipe"))
            .await.expect("Could not open named pipe")
    }

    async fn md_to_pipe_with0(pipe: &mut tokio::fs::File, md: &mut Vec<u8>) {
        md.push(0);
        Self::md_to_pipe(pipe, &md).await;
        md.pop();
    }

    async fn md_to_pipe(pipe: &mut tokio::fs::File, md: &[u8]) {
        let mut written = 0;
        let mut last_n = 0;
        // TODO: This is `<=` not `<` so we can catch the potential `WouldBlock` error
        //       for the last write. Because this apparently fires only in the next
        //       (then empty) `write`?!
        //       (see the `WTF` at `written -= last_n`)
        while written <= md.len() {
            match pipe.write(&md[written..]).await {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    trace!("GOT WouldBlock {} after {} bytes", e, written);
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                    // TODO: I don't understand this. But this seems to give the correct results?!
                    written -= last_n;
                },
                Err(e) => panic!("{}", e),
                Ok(n) => {
                    // TODO: See comment above `while(...)`
                    if written >= md.len() {
                        trace!("written (end) = {} (added {})", written, n);
                        break;
                    }
                    last_n = n;
                    written += n;
                    trace!("written = {} (added {})", written, n);
                }
            }
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if self.is_python {
            // Python version spawns additional processes that won't be killed by child.kill()
            std::process::Command::new("pmpm")
                .arg("--stop")
                .arg("--port").arg(self.port.to_string())
                .spawn().unwrap().wait().unwrap();
        }
    }
}

#[tokio::test]
async fn basic_pipe_input_websocket_response() -> Result<()> {
    let mut ts = TestServer::new(false).await?;

    let mut pipe = ts.open_pipe().await;

    pipe.write_all(b"# hi\n\nhello\0").await?;
    
    let msg = ts.ws_stream.next().await.context("no websocket msg")??;
    let msg: NewContentMessage = serde_json::from_str(msg.to_text()?)?;
    assert!(msg.filepath.ends_with("LIVE"));
    assert_eq!(msg.toc, false);
    assert_eq!(msg.suppress_bibliography, false);
    assert_eq!(msg.toc_title, None);
    assert_eq!(msg.reference_section_title, "");
    assert_eq!(msg.bibid, None);
    assert_eq!(msg.htmlblocks.len(), 2);
    assert_eq!(msg.htmlblocks[0].html, "<h1 id=\"hi\">hi</h1>\n");
    assert_eq!(msg.htmlblocks[1].html, "<p>hello</p>\n");
    assert_ne!(msg.htmlblocks[0].hash, msg.htmlblocks[1].hash);

    Ok(())
}

#[tokio::test]
async fn python_basic() -> Result<()> {
    let mut ts = TestServer::new_python().await?;

    let mut pipe = ts.open_pipe().await;

    pipe.write_all(b"# hi\n\nhello\0").await?;
    
    let msg = loop {
        let msg = ts.ws_stream.next().await.context("no websocket msg")??;
        if !msg.to_text()?.starts_with("{\"html") {
            break msg;
        }
    };
    let msg: PyNewContentMessage = serde_json::from_str(msg.to_text()?)?;
    assert!(msg.filepath.ends_with("LIVE"));
    assert_eq!(msg.toc, false);
    assert_eq!(msg.suppress_bibliography, false);
    assert_eq!(msg.toc_title, None);
    assert_eq!(msg.reference_section_title, "");
    assert_eq!(msg.bibid, None);
    assert_eq!(msg.htmlblocks.len(), 2);
    assert_eq!(msg.htmlblocks[0].html, "<h1 id=\"hi\">hi</h1>\n");
    assert_eq!(msg.htmlblocks[1].html, "<p>hello</p>\n");
    assert_ne!(msg.htmlblocks[0].hash, msg.htmlblocks[1].hash);

    Ok(())
}

