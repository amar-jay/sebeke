use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use tokio::sync::mpsc;
use tokio::time;

use zenoh::bytes::Encoding;

#[derive(Clone)]
struct AppState {
    messages: Vec<String>,
    input: String,
    topic: String,
}

enum AppEvent {
    Input(Event),
    Tick,
    Message(String),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 1. Setting up Zenoh Session
    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let workerd = WorkerRelay::new(session.clone());
    let topic = "chat".to_string();

    let subscriber = session.declare_subscriber(topic.clone()).await.unwrap();

    let (tx, mut rx) = mpsc::channel(100);

    // 2. Background task to receive Zenoh messages
    let tx_zenoh = tx.clone();
    tokio::spawn(async move {
        while let Ok(sample) = subscriber.recv_async().await {
            if let Ok(msg) = String::from_utf8(sample.payload().to_bytes().into_owned()) {
                let _ = tx_zenoh.send(AppEvent::Message(msg)).await;
            }
        }
    });

    // 3. Background task to read keyboard inputs and ticks
    let tx_input = tx.clone();
    tokio::spawn(async move {
        let tick_rate = Duration::from_millis(250);
        loop {
            if event::poll(tick_rate).unwrap() {
                if let Ok(evt) = event::read() {
                    let _ = tx_input.send(AppEvent::Input(evt)).await;
                }
            } else {
                let _ = tx_input.send(AppEvent::Tick).await;
            }
        }
    });

    // 4. Terminal setup for Ratatui
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = AppState {
        messages: Vec::new(),
        input: String::new(),
        topic,
    };

    let res = run_app(&mut terminal, &mut app, &mut rx, session).await;

    // 5. Terminal teardown
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

async fn run_app<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut AppState,
    rx: &mut mpsc::Receiver<AppEvent>,
    session: Arc<zenoh::Session>,
) -> io::Result<()> 
where std::io::Error: From<<B as Backend>::Error>
{
    loop {
        // Draw the UI
        terminal.draw(|f| ui(f, app))?;

        // Handle events
        if let Some(event) = rx.recv().await {
            match event {
                AppEvent::Input(Event::Key(key)) => {
                    match key.code {
                        KeyCode::Char(c) => app.input.push(c),
                        KeyCode::Backspace => {
                            app.input.pop();
                        }
                        KeyCode::Enter => {
                            let msg = std::mem::take(&mut app.input);
                            if !msg.is_empty() {
                                // Publish message to Zenoh
                                let _ = session.put(&app.topic, msg)
                                    .encoding(Encoding::TEXT_PLAIN)
                                    .await;
                            }
                        }
                        KeyCode::Esc => {
                            return Ok(()); // Quit
                        }
                        _ => {}
                    }
                }
                AppEvent::Message(msg) => {
                    app.messages.push(msg); // process incoming Zenoh message
                }
                AppEvent::Tick => {}
                _ => {}
            }
        }
    }
}

fn ui(f: &mut Frame, app: &AppState) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Min(0), Constraint::Length(3)])
        .split(f.area());

    let messages: Vec<ListItem> = app
        .messages
        .iter()
        .map(|m| ListItem::new(Line::from(Span::raw(format!(">>> {}", m)))))
        .collect();

    let messages_list = List::new(messages)
        .block(Block::default().borders(Borders::ALL).title(format!(" Received Messages on topic '{}' ", app.topic)));
        
    f.render_widget(messages_list, layout[0]);

    let input_par = Paragraph::new(app.input.as_str())
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().borders(Borders::ALL).title(" Your Message (Enter: Send, Esc: Quit) "));
        
    f.render_widget(input_par, layout[1]);
}
