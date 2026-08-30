use std::io;

use tokio::{select, sync::mpsc};

use crate::{
    ai,
    chat::Chat,
    config::Config,
    telegram::TelegramBot,
    types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage},
};

pub struct App {
    config: Config,
    prompt: String,
}

impl App {
    pub fn new(config: Config, prompt: String) -> Self {
        Self { config, prompt }
    }

    pub async fn ask(&self, message: String) -> Result<(), Box<dyn std::error::Error>> {
        let (user_tx, user_rx) = mpsc::channel(1);
        let (assistant_tx, assistant_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(assistant_tx, user_rx) => result,
            result = self.run_ask(message, user_tx, assistant_rx) => result,
        }
    }

    pub async fn chat(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (user_tx, user_rx) = mpsc::channel(1);
        let (assistant_tx, assistant_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(assistant_tx, user_rx) => result,
            result = Chat::new().run(user_tx, assistant_rx) => result,
        }
    }

    pub async fn telegram_bot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (user_tx, user_rx) = mpsc::channel(1);
        let (assistant_tx, assistant_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(assistant_tx, user_rx) => result,
            result = self.run_telegram_bot(user_tx, assistant_rx) => result,
        }
    }

    async fn run_ask(
        &self,
        message: String,
        tx: mpsc::Sender<UserMessage>,
        mut rx: mpsc::Receiver<AssistantMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
        println!("{content}");

        Ok(())
    }

    async fn run_ai(
        &self,
        tx: mpsc::Sender<AssistantMessage>,
        rx: mpsc::Receiver<UserMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ai_config = &self.config.ai;

        ai::run(
            &ai_config.api_base,
            &ai_config.api_key,
            &ai_config.model,
            &self.prompt,
            tx,
            rx,
        )
        .await
    }

    async fn run_telegram_bot(
        &self,
        tx: mpsc::Sender<UserMessage>,
        rx: mpsc::Receiver<AssistantMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let telegram_config = self.config.telegram.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing telegram config")
        })?;

        let bot = TelegramBot::connect(&telegram_config.bot_token).await?;
        bot.run(tx, rx).await
    }
}
