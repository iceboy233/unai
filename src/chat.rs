use std::{
    env,
    io::{self, IsTerminal},
};

use anstyle::{AnsiColor, Color, Style};
use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc,
};

use crate::types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage};

pub async fn run(
    tx: mpsc::Sender<UserMessage>,
    mut rx: mpsc::Receiver<AssistantMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    const STYLE_YOU: Style = Style::new()
        .fg_color(Some(Color::Ansi(AnsiColor::BrightBlue)))
        .bold();
    const STYLE_AI: Style = Style::new()
        .fg_color(Some(Color::Ansi(AnsiColor::Blue)))
        .bold();

    let use_style = io::stdout().is_terminal()
        && env::var_os("NO_COLOR").is_none()
        && env::var_os("TERM").is_none_or(|term| term != "dumb");
    let style_you = use_style.then_some(STYLE_YOU).unwrap_or_default();
    let style_ai = use_style.then_some(STYLE_AI).unwrap_or_default();
    let prompt_you = format!("{style_you}You >{style_you:#} ");
    let prompt_ai = format!("{style_ai} AI >{style_ai:#} ");

    let mut lines = BufReader::new(stdin()).lines();
    let mut writer = BufWriter::new(stdout());

    loop {
        writer.write_all(prompt_you.as_bytes()).await?;
        writer.flush().await?;

        let Some(message) = lines.next_line().await? else {
            writer.write_all(b"\n").await?;
            writer.flush().await?;
            return Ok(());
        };

        tx.send(UserMessage {
            session: SessionId(Platform::Local, 0),
            user: User::default(),
            content: Content::Text(message),
            should_reply: true,
        })
        .await?;

        let message = rx
            .recv()
            .await
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        let Content::Text(content) = message.content;
        writer.write_all(prompt_ai.as_bytes()).await?;
        writer.write_all(content.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
}
