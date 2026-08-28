use std::{env, fs, io, path::PathBuf};

use bpaf::Bpaf;
use tokio::sync::mpsc;
use unai::{
    ai,
    config::Config,
    telegram,
    types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage},
};

#[derive(Clone, Debug, Bpaf)]
#[bpaf(options, version)]
struct Options {
    /// Config file path in TOML format
    #[bpaf(short, long)]
    config: PathBuf,

    /// Prompt file path
    #[bpaf(short, long)]
    prompt: PathBuf,

    #[bpaf(external)]
    command: Command,
}

#[derive(Clone, Debug, Bpaf)]
enum Command {
    /// Ask a question
    #[bpaf(command("ask"))]
    Ask {
        /// User message
        #[bpaf(positional("MESSAGE"))]
        message: String,
    },

    /// Run Telegram bot
    #[bpaf(command("telegram-bot"))]
    TelegramBot,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .env()
        .init()?;
    let options = options().run();
    let config: Config = toml::from_str(&fs::read_to_string(&options.config)?)?;
    let prompt = fs::read_to_string(&options.prompt)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match options.command {
        Command::Ask { message } => runtime.block_on(ask(&config, &prompt, &message)),
        Command::TelegramBot => runtime.block_on(telegram_bot(&config, &prompt)),
    }
}

async fn ask(
    config: &Config,
    prompt: &str,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (user_tx, user_rx) = mpsc::channel(1);
    let (assistant_tx, assistant_rx) = mpsc::channel(1);

    tokio::select! {
        result = ai::run(
            &config.ai.api_base,
            &config.ai.api_key,
            &config.ai.model,
            prompt,
            assistant_tx,
            user_rx,
        ) => result,
        result = run_ask(message, user_tx, assistant_rx) => result,
    }
}

async fn telegram_bot(config: &Config, prompt: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (user_tx, user_rx) = mpsc::channel(1);
    let (assistant_tx, assistant_rx) = mpsc::channel(1);

    tokio::select! {
        result = ai::run(
            &config.ai.api_base,
            &config.ai.api_key,
            &config.ai.model,
            prompt,
            assistant_tx,
            user_rx,
        ) => result,
        result = telegram::run(&config.telegram.bot_token, user_tx, assistant_rx) => result,
    }
}

async fn run_ask(
    message: &str,
    tx: mpsc::Sender<UserMessage>,
    mut rx: mpsc::Receiver<AssistantMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    tx.send(UserMessage {
        session: SessionId(Platform::Local, 0),
        user: User::default(),
        content: Content::Text(message.to_string()),
        should_reply: true,
    })
    .await?;

    let message = rx
        .recv()
        .await
        .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
    let Content::Text(content) = message.content;
    println!("{content}");

    Ok(())
}
