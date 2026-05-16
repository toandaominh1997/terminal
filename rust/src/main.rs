use std::collections::{HashMap, VecDeque};
use std::env;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::header,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, Notify};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const READ_BUF: usize = 64 * 1024;
const BROADCAST_CAP: usize = 256;
const HISTORY_CAP_BYTES: usize = 1024 * 1024;

const XTERM_CSS: &str = include_str!("../assets/xterm.css");
const XTERM_JS: &str = include_str!("../assets/xterm.js");
const XTERM_FIT_JS: &str = include_str!("../assets/xterm-addon-fit.js");
const XTERM_LINKS_JS: &str = include_str!("../assets/xterm-addon-web-links.js");

const INDEX_HTML: &str = r#"<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>terminal</title>
<link rel="stylesheet" href="/assets/xterm.css" />
<style>
  html, body { margin: 0; padding: 0; height: 100%; background: #000; }
  #term { width: 100vw; height: 100vh; }
</style>
</head>
<body>
<div id="term"></div>
<script src="/assets/xterm.js"></script>
<script src="/assets/xterm-addon-fit.js"></script>
<script src="/assets/xterm-addon-web-links.js"></script>
<script>
  const term = new Terminal({
    cursorBlink: true,
    fontFamily: 'Menlo, Monaco, "Courier New", monospace',
    fontSize: 13,
    scrollback: 5000,
    theme: { background: '#000000' },
  });
  const fit = new FitAddon.FitAddon();
  term.loadAddon(fit);
  term.loadAddon(new WebLinksAddon.WebLinksAddon());
  term.open(document.getElementById('term'));
  fit.fit();

  // Stable per-browser session id so page reload reattaches to the same shell.
  let sid = localStorage.getItem('xterm-session');
  if (!sid) {
    sid = (crypto.randomUUID && crypto.randomUUID()) ||
          (Date.now().toString(36) + Math.random().toString(36).slice(2));
    localStorage.setItem('xterm-session', sid);
  }

  const enc = new TextEncoder();
  const wsProto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(wsProto + '://' + location.host + '/ws?session=' + encodeURIComponent(sid));
  ws.binaryType = 'arraybuffer';

  function sendResize() {
    ws.send(JSON.stringify({type:'resize', rows: term.rows, cols: term.cols}));
  }

  ws.onopen = () => {
    sendResize();
    term.onData(d => ws.send(enc.encode(d)));
    term.onResize(() => sendResize());
  };
  ws.onmessage = (ev) => {
    if (typeof ev.data === 'string') return;
    term.write(new Uint8Array(ev.data));
  };
  ws.onclose = () => term.write('\r\n[connection closed]\r\n');

  window.addEventListener('resize', () => fit.fit());
</script>
</body>
</html>
"#;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClientCtrl {
    Resize { rows: u16, cols: u16 },
}

#[derive(Default)]
struct History {
    chunks: VecDeque<Vec<u8>>,
    bytes: usize,
}

impl History {
    fn push(&mut self, chunk: &[u8]) {
        self.bytes += chunk.len();
        self.chunks.push_back(chunk.to_vec());
        while self.bytes > HISTORY_CAP_BYTES {
            match self.chunks.pop_front() {
                Some(c) => self.bytes -= c.len(),
                None => break,
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes);
        for c in &self.chunks {
            out.extend_from_slice(c);
        }
        out
    }
}

struct Session {
    pty_master: Mutex<Box<dyn MasterPty + Send>>,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    history: Mutex<History>,
    output: broadcast::Sender<Vec<u8>>,
    done: Arc<Notify>,
}

type SessionMap = Arc<Mutex<HashMap<String, Arc<Session>>>>;

#[derive(Clone)]
struct AppState {
    sessions: SessionMap,
}

fn spawn_session(sessions: SessionMap, id: String) -> anyhow::Result<Arc<Session>> {
    let pair = native_pty_system().openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let shell = env::var("XTERM_SHELL")
        .or_else(|_| env::var("SHELL"))
        .unwrap_or_else(|_| "/bin/zsh".to_string());
    let mut cmd = CommandBuilder::new(&shell);
    cmd.env("TERM", "xterm-256color");
    for k in ["HOME", "PATH", "LANG", "LC_ALL", "USER", "LOGNAME", "TZ"] {
        if let Ok(v) = env::var(k) {
            cmd.env(k, v);
        }
    }
    if let Ok(home) = env::var("HOME") {
        cmd.cwd(home);
    }
    let mut child = pair.slave.spawn_command(cmd)?;
    drop(pair.slave);

    let master: Box<dyn MasterPty + Send> = pair.master;
    let mut reader = master.try_clone_reader()?;
    let mut writer = master.take_writer()?;

    let (output_tx, _) = broadcast::channel::<Vec<u8>>(BROADCAST_CAP);
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let session = Arc::new(Session {
        pty_master: Mutex::new(master),
        write_tx,
        history: Mutex::new(History::default()),
        output: output_tx,
        done: Arc::new(Notify::new()),
    });

    // PTY writer thread.
    std::thread::Builder::new()
        .name(format!("pty-writer:{id}"))
        .spawn(move || {
            while let Some(data) = write_rx.blocking_recv() {
                if writer.write_all(&data).is_err() {
                    break;
                }
            }
        })?;

    // PTY reader thread. Append-then-broadcast under the history lock so any new
    // subscriber sees each chunk in either the snapshot OR the broadcast, never both.
    let sess_reader = session.clone();
    let id_for_reader = id.clone();
    std::thread::Builder::new()
        .name(format!("pty-reader:{id}"))
        .spawn(move || {
            let mut buf = vec![0u8; READ_BUF];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let chunk = &buf[..n];
                        let mut h = sess_reader.history.lock().unwrap();
                        h.push(chunk);
                        let _ = sess_reader.output.send(chunk.to_vec());
                    }
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            sessions.lock().unwrap().remove(&id_for_reader);
            sess_reader.done.notify_waiters();
        })?;

    Ok(session)
}

#[derive(Deserialize)]
struct WsQuery {
    session: Option<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, q.session, state))
}

async fn handle_socket(socket: WebSocket, session_id: Option<String>, state: AppState) {
    let id = session_id
        .filter(|s| !s.is_empty() && s.len() <= 128 && s.chars().all(|c| c.is_ascii_graphic()))
        .unwrap_or_else(|| format!("anon-{}", rand_id()));

    let session = {
        let mut map = state.sessions.lock().unwrap();
        if let Some(s) = map.get(&id) {
            s.clone()
        } else {
            match spawn_session(state.sessions.clone(), id.clone()) {
                Ok(s) => {
                    map.insert(id.clone(), s.clone());
                    s
                }
                Err(e) => {
                    tracing::warn!("spawn session {id}: {e}");
                    return;
                }
            }
        }
    };

    if let Err(e) = run_attach(socket, session).await {
        tracing::warn!("ws {id} ended: {e}");
    }
}

async fn run_attach(socket: WebSocket, session: Arc<Session>) -> anyhow::Result<()> {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Atomic subscribe + snapshot: same lock the reader holds across push+broadcast.
    let (snapshot, mut rx) = {
        let h = session.history.lock().unwrap();
        let rx = session.output.subscribe();
        (h.snapshot(), rx)
    };

    let done = session.done.clone();
    let pty_to_ws = tokio::spawn(async move {
        if !snapshot.is_empty() && ws_tx.send(Message::Binary(snapshot)).await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                biased;
                r = rx.recv() => match r {
                    Ok(chunk) => {
                        if ws_tx.send(Message::Binary(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = done.notified() => break,
            }
        }
        let _ = ws_tx.close().await;
    });

    while let Some(msg) = ws_rx.next().await {
        let Ok(msg) = msg else { break };
        match msg {
            Message::Binary(data) => {
                if session.write_tx.send(data).is_err() {
                    break;
                }
            }
            Message::Text(text) => {
                if let Ok(ClientCtrl::Resize { rows, cols }) =
                    serde_json::from_str::<ClientCtrl>(&text)
                {
                    if let Ok(m) = session.pty_master.lock() {
                        let _ = m.resize(PtySize {
                            rows,
                            cols,
                            pixel_width: 0,
                            pixel_height: 0,
                        });
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    pty_to_ws.abort();
    Ok(())
}

fn rand_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{n:x}")
}

fn css(body: &'static str) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], body)
}

fn js(body: &'static str) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript; charset=utf-8")],
        body,
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let host = env::var("XTERM_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = env::var("XTERM_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8000);

    let state = AppState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/assets/xterm.css", get(|| async { css(XTERM_CSS) }))
        .route("/assets/xterm.js", get(|| async { js(XTERM_JS) }))
        .route(
            "/assets/xterm-addon-fit.js",
            get(|| async { js(XTERM_FIT_JS) }),
        )
        .route(
            "/assets/xterm-addon-web-links.js",
            get(|| async { js(XTERM_LINKS_JS) }),
        )
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("serving xterm on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
