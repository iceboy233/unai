use std::{
    env,
    io::{self, IsTerminal},
};

use anstyle::{AnsiColor, Color, Style};
use tokio::{
    io::{
        stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout,
    },
    select,
    signal::ctrl_c,
    sync::mpsc,
};

use crate::types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage};

pub struct Chat {
    lines: Lines<BufReader<Stdin>>,
    writer: BufWriter<Stdout>,
    prompt_you: String,
    prompt_ai: String,
    tx: mpsc::Sender<UserMessage>,
    rx: mpsc::Receiver<AssistantMessage>,
}

enum Line {
    Message(String),
    Retry,
    End,
}

impl Chat {
    pub fn new(tx: mpsc::Sender<UserMessage>, rx: mpsc::Receiver<AssistantMessage>) -> Self {
        const STYLE_YOU: Style = Style::new()
            .fg_color(Some(Color::Ansi(AnsiColor::BrightBlue)))
            .bold();
        const STYLE_AI: Style = Style::new()
            .fg_color(Some(Color::Ansi(AnsiColor::Blue)))
            .bold();

        let lines = BufReader::new(stdin()).lines();
        let writer = BufWriter::new(stdout());
        let use_style = io::stdout().is_terminal()
            && env::var_os("NO_COLOR").is_none()
            && env::var_os("TERM").is_none_or(|term| term != "dumb");
        let style_you = use_style.then_some(STYLE_YOU).unwrap_or_default();
        let style_ai = use_style.then_some(STYLE_AI).unwrap_or_default();
        let prompt_you = format!("{style_you}You >{style_you:#} ");
        let prompt_ai = format!("{style_ai} AI >{style_ai:#} ");

        Self {
            lines,
            writer,
            prompt_you,
            prompt_ai,
            tx,
            rx,
        }
    }

    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            self.writer.write_all(self.prompt_you.as_bytes()).await?;
            self.writer.flush().await?;

            let message = match self.read_line().await? {
                Line::Message(message) => message,
                Line::Retry => {
                    self.writer.write_all(b"\n").await?;
                    continue;
                }
                Line::End => {
                    self.writer.write_all(b"\n").await?;
                    self.writer.flush().await?;
                    return Ok(());
                }
            };

            self.tx
                .send(UserMessage {
                    session: SessionId(Platform::Local, 0),
                    user: User::default(),
                    content: Content::Text(message),
                    should_reply: true,
                })
                .await?;

            let message = self
                .rx
                .recv()
                .await
                .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
            self.writer.write_all(self.prompt_ai.as_bytes()).await?;
            self.write_content(message.content).await?;
        }
    }

    async fn read_line(&mut self) -> io::Result<Line> {
        select! {
            line = self.lines.next_line() => {
                match line? {
                    Some(message) => Ok(Line::Message(message)),
                    None => Ok(Line::End),
                }
            }
            signal = ctrl_c() => {
                signal?;
                Ok(Line::Retry)
            }
        }
    }

    async fn write_content(&mut self, content: Content) -> io::Result<()> {
        let Content::Text(text) = content;
        self.writer.write_all(text.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }
}
