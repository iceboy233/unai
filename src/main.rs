use std::{env, fs, path::PathBuf};

use bpaf::Bpaf;
use unai::{app::App, config::Config};

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

    /// Start an interactive chat
    #[bpaf(command("chat"))]
    Chat,

    /// Run Telegram bot
    #[bpaf(command("telegram-bot"))]
    TelegramBot,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()?;
    let options = options().run();
    let config: Config = toml::from_str(&fs::read_to_string(&options.config)?)?;
    let prompt = fs::read_to_string(&options.prompt)?;
    let app = App::new(config, prompt);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match options.command {
        Command::Ask { message } => runtime.block_on(app.ask(message)),
        Command::Chat => runtime.block_on(app.chat()),
        Command::TelegramBot => runtime.block_on(app.telegram_bot()),
    }
}
