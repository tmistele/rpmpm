mod auth;

use crate::md::md2htmlblocks;

use std::net::{SocketAddr, Ipv4Addr};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::path::{PathBuf, Path};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::{RawFd, IntoRawFd, FromRawFd};

use libsystemd::activation::IsType;

use anyhow::{Context, Result, anyhow};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

use serde::Serialize;

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

async fn handle_connection(peer_map: PeerMap, stream: TcpStream, addr: SocketAddr, secret: String) -> Result<()>  {
    println!("incoming connectoin");
    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("ws connected");

    // First authenticate client, then work with it
    {
        let auth_result = auth::try_auth_client(&mut ws_stream, &secret).await;
        if let Ok(_) = auth_result {
            // ok
        } else {
            // not ok
            return auth_result;
        }
    }

    // Add client to list of clients
    let (tx, rx) = unbounded();
    peer_map.lock().unwrap().insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();
    
    // Wait for stream close
    let wait_incoming = incoming.try_for_each(|msg| {
        // TODO: process file path request
        // TODO: process citeproc request
        println!("Received a message from {}: {}", addr, msg.to_text().unwrap());
        future::ok(())
    });

    // Wait for message and send to client
    let wait_forward = rx.map(Ok).forward(outgoing);

    pin_mut!(wait_forward, wait_incoming);
    future::select(wait_forward, wait_incoming).await;

    println!("{} disconnected", &addr);
    peer_map.lock().unwrap().remove(&addr);

    Ok(())
}

#[derive(Serialize, Debug)]
struct StatusMsg<'a> {
    status: &'a str
}

async fn progressbar(peer_map: PeerMap) -> Result<()> {
    let mut i = 0;
    let duration = tokio::time::Duration::from_millis(300);
    loop {
        tokio::time::sleep(duration).await;
        i += 1;
        let msg = Message::text(serde_json::to_string(&StatusMsg{status: &" 🞄 ".repeat(i)})?);
        {
            let peers = peer_map.lock().unwrap();
            for (_, ws_sink) in peers.iter() {
                println!("sending PROGRESS to a client!!");
                ws_sink.unbounded_send(msg.clone())?;
            }
        }
    }
}

#[derive(Serialize, Debug)]
struct ErrMsg {
    error: String
}

struct NewContent {
    fpath: PathBuf,
    md: Vec<u8>,
}

struct QueueStatus {
    processing: bool,
    new_content: Option<NewContent>,
}

type Queue = Arc<Mutex<QueueStatus>>;
async fn process_new_content(peer_map: PeerMap, queue: Queue, mut new: NewContent) -> Result<()> {
    loop {
        // Process current content
        {
            let progressbar = tokio::spawn(progressbar(peer_map.clone()));
            let jsonmessage = md2htmlblocks(
                new.md,
                new.fpath.as_path(),
                new.fpath.parent().context("could not get parent of filepath")?
            ).await;
            progressbar.abort();

            let (msg, citeproc_handle) = match jsonmessage {
                Err(ref e) => (serde_json::to_string(&ErrMsg { error: format!("{}", e)})?, None),
                Ok((jsonmessage, citeproc_handle)) => (jsonmessage, Some(citeproc_handle))
            };
            let msg = Message::text(msg);

            {
                let peers = peer_map.lock().unwrap();
                for (_, ws_sink) in peers.iter() {
                    println!("sending to a client!!");
                    ws_sink.unbounded_send(msg.clone())?;
                }
            }

            // citeproc
            // Note: We await citeproc only after sending jsonmessage to client
            // to minimize time until client first gets results
            if let Some(citeproc_handle) = citeproc_handle {
                let citeproc_json = citeproc_handle.await?;
                let msg = Message::text(citeproc_json);

                let peers = peer_map.lock().unwrap();
                for (_, ws_sink) in peers.iter() {
                    println!("sending CITEPROC to a client!!");
                    ws_sink.unbounded_send(msg.clone())?;
                }

            }
        }

        // Get next content to process, if any
        new = {
            let mut status = queue.lock().unwrap();
            let next = status.new_content.take();
            if let Some(next) = next {
                next
            } else {
                status.processing = false;
                break;
            }
        };
    }

    Ok(())
}

// A simpler design would be to get rid of field `processing` and
// - always put new content in queue
// - unconditionally call `tokio::spawn(process_new_content())`
// - `process_new_content` then first checks if new content is in queue and
//   takes content from queue instead of as direct function parameter.
//
// The design here is a bit more complicated but has two advantages:
// - do not `tokio::spawn()` unnecessarily when already processing
// - the fast path "content comes in + is processed right away" does not
//   take the lock two times right after each other (once here for insert
//   in queue + once at the beginning of `process_new_content`)
//
fn submit_new_content(peer_map: &PeerMap, queue: &Queue, new: NewContent) {
    {
        let mut status = queue.lock().unwrap();
        if status.processing {
            println!("Still BUSY - putting it in queue.");
            status.new_content = Some(new);
            return;
        } else {
            status.processing = true;
        }
    };

    tokio::spawn(process_new_content(peer_map.clone(), queue.clone(), new));
}


fn new_pipe_content(peer_map: &PeerMap, queue: &Queue, home: &Path, buf: &bytes::BytesMut) -> Result<()> {
    // parse '<!-- filepath:... -->\n'
    let (content, fpath) = if buf.starts_with(b"<!-- filepath:") {
        let lineend = &buf[14..].iter().position(|&x| x == b'\n');
        if let Some(lineend) = lineend {
            let split = 14+*lineend+1;
            (&buf[split..], home.join(std::str::from_utf8(&buf[14..split-5])?))
        } else {
            (&buf[..], home.join("LIVE"))
        }
    } else {
        (&buf[..], home.join("LIVE"))
    };

    let new = NewContent {
        fpath: fpath,
        md: content.to_vec()
    };

    submit_new_content(peer_map, queue, new);

    Ok(())
}

async fn monitorpipe(file: Option<tokio::fs::File>, pipe: PathBuf, peer_map: PeerMap, queue: Queue, home: PathBuf) -> Result<()> {

    // TODO: increase pipe buf size? (match 65_535?)

    let mut buf = bytes::BytesMut::with_capacity(65_535);
    let mut _start = std::time::Instant::now();

    // Start with pre-opened file from systemd fd, if any
    let mut file = if let Some(file) = file {
        file
    } else {
        println!("Opening pipe from path");
        match tokio::fs::File::open(&pipe).await {
            Ok(file) => file,
            Err(e) => return Err(anyhow!(e)),
        }
    };

    loop {
        let mut pipe_stream = tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new());
        while let Some(read) = pipe_stream.next().await {
            let data = read?;
            if buf.len() == 0 {
                println!("start reading into new buf...");
                _start = std::time::Instant::now();
            }

            // Don't add \0 to buf
            let (received_0, data) = if data.last().unwrap() == &0 {
                (true, &data[..data.len()-1])
            } else {
                (false, &data[..])
            };

            println!("got data: {}", data.len());
            buf.extend_from_slice(&data);

            // Trigger on \0
            if received_0 {
                println!("read pipe total = {:?}", _start.elapsed());
                _start = std::time::Instant::now();
                println!("GOT \\0 - total: {}", buf.len());

                new_pipe_content(&peer_map, &queue, &home, &buf)?;

                // Take previous size as new estimate
                buf = bytes::BytesMut::with_capacity(buf.len());
            }
        };

        // Also trigger on EOF
        println!("got EOF - total: {}", buf.len());
        if !buf.is_empty() {
            println!("read pipe total = {:?}", _start.elapsed());
            _start = std::time::Instant::now();
            println!("GOT EOF - total: {}", buf.len());

            new_pipe_content(&peer_map, &queue, &home, &buf)?;

            // Take previous size as new estimate
            buf = bytes::BytesMut::with_capacity(buf.len());
        }

        // Reopen file
        //
        // NB: We could try to be clever and reuse the fd which might be interesting
        // in the socket-activated case. We could use this:
        //
        // `pipe_stream.into_inner().into_std().await.into_raw_fd()`
        //
        // But first: In practice, the pipe passed from systemd never EOFs, not even
        // if a clients disconnects.
        //
        // And second: It has already-eofd by then and reusing it will just result in
        // a busy loop until the next client connects (if the EOF came about due to
        // client disconnect at least...).
        file = tokio::fs::File::open(&pipe).await?;
    }
}

fn read_socket_activation_fds() -> (Option<RawFd>,Option<RawFd>) {
    // We use `unset_env = true` to prevent accidentally using the same `RawFd` twice
    if let Ok(fds) = libsystemd::activation::receive_descriptors(true /* unset_env */) {
        // TODO: Do better validation?
        let mut pipe = None;
        let mut websocket = None;
        for fd in fds {
            if fd.is_fifo() {
                pipe = Some(fd.into_raw_fd());
            } else if fd.is_inet() {
                websocket = Some(fd.into_raw_fd());
            }
        }
        (pipe, websocket)
    } else {
        (None, None)
    }
}

fn print_client_autodiscovery(port: u16, secret: &str,  runtime_dir: &Path, pipe_path: &Path) -> Result<()> {

    // TODO: CARGO_MANIFEST_DIR only works when run with `cargo run`. What to do otherwise?
    let base_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);

    let client_path = std::fs::canonicalize(base_dir.join("src/../client/pmpm_revealjs.html"))?;
    let client_path_str = client_path.to_str().context("could not convert client_path to str")?;
    std::fs::write(runtime_dir.join("client_path_revealjs"), &client_path_str)?;

    let client_path = std::fs::canonicalize(base_dir.join("src/../client/pmpm.html"))?;
    let client_path_str = client_path.to_str().context("could not convert client_path to str")?;
    std::fs::write(runtime_dir.join("client_path"), &client_path_str)?;

    std::fs::write(runtime_dir.join("websocket_secret"), &secret)?;
    std::fs::write(runtime_dir.join("websocket_port"), format!("{}", port))?;

    let pipe_path_str = pipe_path.to_str().context("could not convert pipe path to str")?;
    println!("pmpm-websocket started (port {})\n\
              \n\
              Pipe new content to {}, for example\n\
                  echo '# Hello World!' > {}\n\
              \n\
              Direct your browser to\n\
                  file://{}?secret={}{}\n\
              to view the rendered markdown",
              port, pipe_path_str, pipe_path_str, client_path_str, secret,
              if port == 9877 { "".to_string() } else { format!("&port={}", port) });

    Ok(())
}

pub async fn run(args: crate::Args) -> Result<()> {
    let port = args.port;
    let server_addr = Ipv4Addr::new(127, 0, 0, 1);

    let home = args.home;
    let runtime_dir = PathBuf::from(std::env::var("XDG_RUNTIME_DIR").unwrap_or("/tmp".to_string())).join("pmpm/");
    let pipe_path = runtime_dir.join("pipe");

    let peer_map = PeerMap::new(Mutex::new(HashMap::new()));
    let queue = Queue::new(Mutex::new(QueueStatus {
        processing: false,
        new_content: None
    }));

    // Maybe get pipe/websocket fds from systemd socket activation
    let (fd_pipe, fd_websocket) = read_socket_activation_fds();

    // generate secret
    let secret = auth::generate_token();

    // Setup runtime dir + pipe
    std::fs::create_dir_all(&runtime_dir)?;
    if let Ok(metadata) = std::fs::metadata(&pipe_path) {
        if !metadata.file_type().is_fifo() {
            return Err(anyhow!("pipe file exists but is not fifo"));
        }
    } else {
        nix::unistd::mkfifo(&pipe_path, nix::sys::stat::Mode::S_IRWXU)?;
    }

    // Start receiving content from pipe
    let file = if let Some(fd) = fd_pipe {
        println!("opening pipe from fd_pipe: {}", fd);
        // SAFETY: The pipe fd is used only here at startup which happens exactly once.
        // If one ever were to change this function `run` to be called twice, then
        // `read_socket_activation_fds()` would not return anything since we set `unset_env`
        // to true there.
        Some(unsafe { tokio::fs::File::from_raw_fd(fd) })
    } else {
        None
    };
    tokio::spawn(monitorpipe(file, pipe_path.clone(), peer_map.clone(), queue.clone(), home.clone()));
    
    // Listener for websocket
    let listener = if let Some(fd) = fd_websocket {
        println!("fd_websocket from systemd: {}!", fd);
        // SAFETY: The websocket fd is used only here at startup which happens exactly once.
        // If one ever were to change this function `run` to be called twice, then
        // `read_socket_activation_fds()` would not return anything since we set `unset_env`
        // to true there.
        let std_listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
        std_listener.set_nonblocking(true)?;
        TcpListener::from_std(std_listener)?
    } else {
        TcpListener::bind((server_addr, port)).await?
    };

    // Write config for auto-discovery in clients
    print_client_autodiscovery(port, &secret, runtime_dir.as_path(), pipe_path.as_path())?;

    // Handle websocket connections
    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(peer_map.clone(), stream, addr, secret.clone()));
    }

    Ok(())
}

