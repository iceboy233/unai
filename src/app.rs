use std::io;

use tokio::{
    select,
    sync::{mpsc, oneshot},
};

use crate::{
    ai::Assistant,
    chat::Chat,
    config::Config,
    telegram::TelegramBot,
    types::{AssistantRequest, Content, Platform, SessionId, User, UserMessage},
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
        let (request_tx, request_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(request_rx) => result,
            result = self.run_ask(message, request_tx) => result,
        }
    }

    pub async fn chat(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (request_tx, request_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(request_rx) => result,
            result = Chat::new().run(request_tx) => result,
        }
    }

    pub async fn telegram_bot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (request_tx, request_rx) = mpsc::channel(1);

        select! {
            result = self.run_ai(request_rx) => result,
            result = self.run_telegram_bot(request_tx) => result,
        }
    }

    async fn run_ai(
        &self,
        request_rx: mpsc::Receiver<AssistantRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ai_config = &self.config.ai;

        let assistant = Assistant::new(
            &ai_config.api_base,
            &ai_config.api_key,
            &ai_config.model,
            &self.prompt,
        );
        assistant.run(request_rx).await
    }

    async fn run_ask(
        &self,
        message: String,
        request_tx: mpsc::Sender<AssistantRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let message = UserMessage {
            session: SessionId(Platform::Local, 0),
            user: User::default(),
            content: Content::Text(message),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = AssistantRequest {
            message,
            reply_tx: Some(reply_tx),
        };
        request_tx.send(request).await?;

        let message = reply_rx.await?;
        let Content::Text(content) = message.content;
        println!("{content}");

        Ok(())
    }

    async fn run_telegram_bot(
        &self,
        request_tx: mpsc::Sender<AssistantRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let telegram_config = self.config.telegram.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing telegram config")
        })?;

        let bot = TelegramBot::connect(&telegram_config.bot_token).await?;
        bot.run(request_tx).await
    }
}
