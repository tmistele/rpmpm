use anyhow::{Context, Result};

use tracing::trace;

use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Message};

use futures::SinkExt;
use futures_util::StreamExt;

use serde::{Deserialize, Serialize};

use rand::distributions::{Alphanumeric, DistString};
use sha2::{Sha512, Digest};

const TOKEN_LENGTH: usize = 50;

pub fn generate_token() -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), TOKEN_LENGTH)
}

fn sha512hex(text: &str) -> String {
    let hash = Sha512::digest(text);
    format!("{:x}", hash)
}


#[derive(Serialize, Debug)]
struct AuthMsgChallenge<'a> {
    challenge: &'a str
}

#[derive(Deserialize, Debug)]
struct AuthMsgResponseClient<'a> {
    hash: &'a str,
    challenge: &'a str,
    cnonce: &'a str
}

#[derive(Serialize, Debug)]
struct AuthMsgResponseServer<'a> {
    hash: &'a str,
    snonce: &'a str
}

#[derive(Serialize, Debug)]
struct AuthFailResponseServer {
    auth: bool
}

#[derive(Debug, Clone)]
struct AuthError;

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Authentication failed")
    }
}

impl std::error::Error for AuthError {}

// This function authenticates a client (websocket). The client must
// authenticate by responding to a challenge before we add it. Similarly, we
// (the server) must authenticate to the client. This should make it safe to
// run pmpm on localhost, even with other untrusted users on the same computer.
// 
// This does not protect against TCP spoofing/MITM/... Thus, pmpm should only
// ever listen on localhost. We assume unprivileged users cannot MITM etc. on
// localhost.
// 
// Even without untrusted users on the same computer, authenticating the client
// is necessary because any website can connect to ws://localhost by default.
// Without client authentication, any website could
// 1) Passively listen to the pandoc output
// 2) Actively choose which files pandoc parses and look at the output
// 
// With untrusted users on the same computer, a concern is untrusted users
// impersonating the pmpm server. To protect against this, we use a form of
// digest authentication for the client authentication. This way the client
// does not leak the secret to a malicious server when trying to authenticate.
// We also authenticate the server to the client so that the client can avoid
// leaking filepaths etc. to a malicious server.
pub async fn try_auth_client(ws_stream: &mut WebSocketStream<TcpStream>, secret: &str) -> Result<()> {

    // First we send the client a challenge to authenticate the client to us ...
    let challenge = generate_token();
    let json = serde_json::to_string(&AuthMsgChallenge {challenge: &challenge})?;
    ws_stream.send(Message::text(json)).await?;

    let response = ws_stream.next().await.context("auth response 1 missing")??;
    let response: AuthMsgResponseClient = serde_json::from_str(response.to_text()?)?;

    if sha512hex(format!("{}{}{}", secret, challenge, response.cnonce).as_str()) == response.hash {
        // ... then we authenticate to the client
        let snonce = generate_token();
        let hash = sha512hex(format!("{}{}{}", secret, response.challenge, snonce).as_str());
        let json = serde_json::to_string(&AuthMsgResponseServer {snonce: &snonce, hash: &hash})?;
        ws_stream.send(Message::text(json)).await?;

        trace!("AUTH SUCCESS");
        Ok(())
    } else {
        let json = serde_json::to_string(&AuthFailResponseServer {auth: false})?;
        ws_stream.send(Message::text(json)).await?;
        Err(AuthError.into())
    }
}

