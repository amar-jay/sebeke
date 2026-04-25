use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use sebeke::relay::{
    worker::WorkerRelay,
    config::{self, Relay},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[path = "../../src/node/mod.rs"]
pub mod node;
use node::Node;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// https://cloudflare.abdelmanan-abdelrahman03.workers.dev

const WORKER_URL: &str = "https://cloudflare.abdelmanan-abdelrahman03.workers.dev";
const TOPIC: &str = "telemetry_chat";

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// Every chat message is serialized as JSON before being put into Zenoh.
/// This lets heterogeneous nodes (different languages / platforms) participate
/// in the same topic as long as they agree on this schema.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct ChatMessage {
    from: String,
    text: String,
    /// Wall-clock time formatted as "HH:MM" at the sender's locale.
    ts: String,
}

impl ChatMessage {
    fn new(from: &str, text: &str, time: Duration) -> Self {
        Self {
            from: from.to_owned(),
            text: text.to_owned(),
            ts: time.is_zero().then(|| Self::get_time_string()).unwrap(),
        }
    }

    fn get_time_string() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let total_seconds = now.as_secs();
        let hours = (total_seconds / 3600) % 24;
        let minutes = (total_seconds / 60) % 60;

        format!("{:02}:{:02}", hours, minutes)
    }

    fn to_json(&self) -> Option<String> {
        serde_json::to_string(self).ok()
    }

    fn from_json(s: &str) -> Option<Self> {
        serde_json::from_str(s).ok()
    }
}

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

struct AppState {
    messages: Vec<ChatMessage>,
    input: String,
    /// Maximum number of `char` code-points allowed in one message.
    input_limit: usize,
    topic: String,
    own_id: String,
    list_state: ListState,
    /// When `true`, a new incoming message scrolls the view to the bottom.
    /// Set to `false` when the user manually scrolls up, re-enabled when
    /// they scroll back to the last message.
    follow_tail: bool,
}

impl AppState {
    fn new(topic: String, own_id: String) -> Self {
        let mut list_state = ListState::default();
        list_state.select(None);
        Self {
            messages: Vec::new(),
            input: String::new(),
            input_limit: 280,
            topic,
            own_id,
            list_state,
            follow_tail: true,
        }
    }

    fn push(&mut self, msg: ChatMessage) {
        self.messages.push(msg);
        if self.follow_tail {
            self.scroll_to_bottom();
        }
    }

    fn scroll_to_bottom(&mut self) {
        let last = self.messages.len().saturating_sub(1);
        if self.messages.is_empty() {
            self.list_state.select(None);
        } else {
            self.list_state.select(Some(last));
        }
        self.follow_tail = true;
    }

    fn scroll_up(&mut self, delta: usize) {
        if self.messages.is_empty() {
            return;
        }
        let current = self
            .list_state
            .selected()
            .unwrap_or_else(|| self.messages.len().saturating_sub(1));
        let next = current.saturating_sub(delta);
        self.list_state.select(Some(next));
        self.follow_tail = next >= self.messages.len().saturating_sub(1);
    }

    fn scroll_down(&mut self, delta: usize) {
        if self.messages.is_empty() {
            return;
        }
        let last = self.messages.len().saturating_sub(1);
        let current = self.list_state.selected().unwrap_or(last);
        let next = (current + delta).min(last);
        self.list_state.select(Some(next));
        self.follow_tail = next >= last;
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

enum AppEvent {
    Input(Event),
    Tick,
    Message(ChatMessage),
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // --- Infrastructure setup ---

    let node = Arc::new(Node::new().await);
    let relay = Arc::new(WorkerRelay::new(
        node.session.clone(),
        WorkerRelay::get_default_config(),
    ));
    let port = std::env::var("PORT").unwrap_or_else(|_| "8787".to_string());
    let local_address = format!("0.0.0.0:{}", port);
    let own_id = node.get_id().await?;
    let telemetry_mode = TOPIC.starts_with("telemetry");

    let worker_cfg = config::CloudflareConfig {
        api_token: "local-dev-token".to_owned(),
        machine_id: own_id.clone(),
        push_path: "/push".to_owned(),
        ws_path: "/ws".to_owned(),
        pull_path: "/pull".to_owned(),
        local_address,
        ..Default::default()
    };

    if telemetry_mode {
        // println!("Starting relay and binding worker on port {}...", port);
        relay
            .bind_worker(WORKER_URL, config::WorkerConfig::Cloudflare(worker_cfg))
            .await?;
    } else {
        // println!(
        //     "Starting relay in websocket-only mode for topic '{}' (skipping bind/tunnel)",
        //     TOPIC
        // );
        relay.attach_worker_ws_only(WORKER_URL, worker_cfg).await?;
    }

    relay.register_proxy(TOPIC, &format!("{}/", WORKER_URL))?;
    let relay_listener = relay.clone();
    tokio::spawn(async move {
        relay_listener.listen().await.expect("Relay server failed");
    });

    // --- Event channel ---

    let (tx, mut rx) = mpsc::channel::<AppEvent>(256);

    // Zenoh subscriber → event channel
    let tx_zenoh = tx.clone();
    node.subscribe(TOPIC, move |raw: String| {
        let sender = tx_zenoh.clone();
        async move {
            if let Some(msg) = ChatMessage::from_json(&raw) {
                let _ = sender.send(AppEvent::Message(msg)).await;
            }
            // Silently ignore non-JSON payloads from other tools/scripts.
        }
    })
    .await?;

    // Keyboard + tick → event channel
    let tx_input = tx.clone();
    tokio::spawn(async move {
        let tick = Duration::from_millis(200);
        loop {
            if event::poll(tick).unwrap_or(false) {
                if let Ok(evt) = event::read() {
                    let _ = tx_input.send(AppEvent::Input(evt)).await;
                }
            } else {
                let _ = tx_input.send(AppEvent::Tick).await;
            }
        }
    });

    // --- Terminal setup ---

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = AppState::new(TOPIC.to_owned(), own_id.to_owned());

    let result = run_app(&mut terminal, &mut app, &mut rx, &node).await;

    // --- Terminal teardown (always runs, even on error) ---

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture,
    )?;
    terminal.show_cursor()?;

    if let Err(e) = result {
        eprintln!("error: {e:?}");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

async fn run_app<B>(
    terminal: &mut Terminal<B>,
    app: &mut AppState,
    rx: &mut mpsc::Receiver<AppEvent>,
    node: &Arc<Node>,
) -> io::Result<()>
where
    B: Backend,
    io::Error: From<B::Error>,
{
    loop {
        terminal.draw(|f| ui(f, app))?;

        let Some(event) = rx.recv().await else {
            break;
        };

        match event {
            // ----------------------------------------------------------------
            // Keyboard
            // ----------------------------------------------------------------
            AppEvent::Input(Event::Key(key)) => {
                // Ctrl-C / Esc → quit
                if key.code == KeyCode::Esc
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL))
                {
                    return Ok(());
                }

                match key.code {
                    // --- Typing ---
                    KeyCode::Char(c) => {
                        if app.input.chars().count() < app.input_limit {
                            app.input.push(c);
                        }
                    }
                    KeyCode::Backspace => {
                        app.input.pop();
                    }

                    // --- Send ---
                    KeyCode::Enter => {
                        let text = std::mem::take(&mut app.input).trim().to_owned();
                        if text.is_empty() {
                            continue;
                        }

                        // Handle local slash-commands before publishing
                        if text == "/quit" || text == "/q" {
                            return Ok(());
                        }

                        let msg = ChatMessage::new(&app.own_id, &text, Duration::ZERO);
                        if let Some(json) = msg.to_json() {
                            // Optimistically show our own message immediately
                            // so the round-trip latency is invisible to the user.
                            app.push(msg);
                            let _ = node.publish(&app.topic, &json).await;
                        }
                    }

                    // --- Scroll ---
                    KeyCode::Up => app.scroll_up(1),
                    KeyCode::Down => app.scroll_down(1),
                    KeyCode::PageUp => app.scroll_up(10),
                    KeyCode::PageDown => app.scroll_down(10),
                    // Home / End jump to first / last message
                    KeyCode::Home => {
                        if !app.messages.is_empty() {
                            app.list_state.select(Some(0));
                            app.follow_tail = false;
                        }
                    }
                    KeyCode::End => app.scroll_to_bottom(),

                    _ => {}
                }
            }

            // ----------------------------------------------------------------
            // Incoming message from Zenoh
            // ----------------------------------------------------------------
            AppEvent::Message(msg) => {
                // Don't double-display our own messages — we already optimistically
                // inserted them above when the user pressed Enter.
                if msg.from != app.own_id {
                    app.push(msg);
                }
            }

            AppEvent::Tick | _ => {}
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn ui(f: &mut Frame, app: &mut AppState) {
    // Three-row layout:
    //   [0] status bar   — 1 line
    //   [1] messages      — fills remaining space
    //   [2] input box     — 3 lines
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .margin(0)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(f.area());

    render_status(f, app, rows[0]);
    render_messages(f, app, rows[1]);
    render_input(f, app, rows[2]);
}

fn render_status(f: &mut Frame, app: &AppState, area: Rect) {
    let scroll_hint = if app.follow_tail {
        Span::raw("")
    } else {
        Span::styled(
            "  ↑ scrolled — ↓/End to follow",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::ITALIC),
        )
    };

    let line = Line::from(vec![
        Span::styled(" ● ", Style::default().fg(Color::Green)),
        Span::styled(
            format!("topic: {}  ", app.topic),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            format!("node: {}  ", app.own_id),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!("{} messages", app.messages.len()),
            Style::default().fg(Color::DarkGray),
        ),
        scroll_hint,
    ]);

    let bar = Paragraph::new(line).style(Style::default().bg(Color::Reset));

    f.render_widget(bar, area);
}

fn render_messages(f: &mut Frame, app: &mut AppState, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            format!(" {} ", app.topic),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));

    let items: Vec<ListItem> = app
        .messages
        .iter()
        .map(|msg| build_list_item(msg, &app.own_id))
        .collect();

    let list = List::new(items)
        .block(block)
        // Invisible highlight — we use ListState only for scroll position,
        // not to visually select items.
        .highlight_style(Style::default());

    f.render_stateful_widget(list, area, &mut app.list_state);
}

fn render_input(f: &mut Frame, app: &AppState, area: Rect) {
    let remaining = app.input_limit.saturating_sub(app.input.chars().count());
    let counter_style = if remaining < 20 {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let title = Line::from(vec![
        Span::raw(" message  "),
        Span::styled(format!("{remaining} left"), counter_style),
        Span::styled(
            "  Enter: send   Esc/Ctrl-C: quit   ↑↓: scroll",
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(" "),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(title);

    // Show a blinking cursor caret appended to the input
    let display = format!("{}▌", app.input);

    let paragraph = Paragraph::new(display.as_str())
        .style(Style::default().fg(Color::White))
        .block(block);

    f.render_widget(paragraph, area);
}

/// Format
/// messages:
///   `[14:32]  you  ›  hey there`  (cyan name)
///   `[14:31]  alice  ›  hello`     (yellow name)
fn build_list_item<'a>(msg: &'a ChatMessage, own_id: &str) -> ListItem<'a> {
    let is_own = msg.from == own_id;

    let name_style = if is_own {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    };

    let text_style = if is_own {
        Style::default().fg(Color::White)
    } else {
        Style::default().fg(Color::Gray)
    };

    let display_name = if is_own {
        "you".to_owned()
    } else {
        msg.from.clone()
    };

    // Pad the sender column to 16 chars so the message bodies line up
    let padded_name = format!("{:<16}", display_name);

    let line = Line::from(vec![
        Span::styled(
            format!(" {:>5}  ", msg.ts),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(padded_name, name_style),
        Span::styled("›  ", Style::default().fg(Color::DarkGray)),
        Span::styled(msg.text.clone(), text_style),
    ]);

    ListItem::new(line)
}
