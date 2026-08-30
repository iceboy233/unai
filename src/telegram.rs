use std::{
    collections::{hash_map, HashMap, VecDeque},
    future::pending,
    time::Duration,
};

use frankenstein::{
    client_reqwest::Bot,
    methods::{GetUpdatesParams, SendChatActionParams, SendMessageParams},
    types::{ChatAction, ChatType, MessageEntityType},
    updates::UpdateContent,
    AsyncTelegramApi, ParseMode,
};
use log::{debug, error, info, warn};
use serde_fmt::to_debug;
use telegram_markdown_v2::UnsupportedTagsStrategy;
use tokio::{
    select,
    sync::mpsc,
    time::{sleep, sleep_until, Instant},
};

use crate::types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage};

pub struct TelegramBot {
    bot: Bot,
    bot_user_id: u64,
    bot_username: String,
}

impl TelegramBot {
    pub async fn connect(bot_token: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let bot = Bot::new(bot_token);

        let bot_user = bot.get_me().await?.result;
        debug!("Got bot user: {:?}", to_debug(&bot_user));
        let bot_user_id = bot_user.id;
        let bot_username = bot_user.username.expect("The bot must have a username.");
        info!("Connected as {bot_username} ({bot_user_id})");

        Ok(Self {
            bot,
            bot_user_id,
            bot_username,
        })
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<UserMessage>,
        rx: mpsc::Receiver<AssistantMessage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (typing_tx, typing_rx) = mpsc::channel(1);
        select! {
            result = self.recv(tx, typing_tx) => result,
            result = self.send(rx, typing_rx) => result,
        }
    }

    async fn recv(
        &self,
        user_tx: mpsc::Sender<UserMessage>,
        typing_tx: mpsc::Sender<i64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut next_offset = None;
        loop {
            let params = GetUpdatesParams::builder()
                .maybe_offset(next_offset)
                .timeout(30)
                .build();
            let updates = loop {
                match self.bot.get_updates(&params).await {
                    Ok(response) => break response.result,
                    Err(e) => {
                        warn!("Get updates failed: {e:?}, retry after 2 seconds");
                        sleep(Duration::from_secs(2)).await;
                    }
                }
            };

            for update in updates {
                next_offset = Some(update.update_id as i64 + 1);
                let message = match update.content {
                    UpdateContent::Message(message) => {
                        let Some(text) = &message.text else {
                            warn!("Unsupported message: {:?}", to_debug(message.as_ref()));
                            continue;
                        };
                        debug!("Received text message: {:?}", to_debug(message.as_ref()));

                        let session = SessionId(Platform::Telegram, message.chat.id);
                        let user = get_user(message.as_ref());
                        let content = Content::Text(text.clone());
                        let should_reply = self.should_reply(message.as_ref());

                        UserMessage {
                            session,
                            user,
                            content,
                            should_reply,
                        }
                    }
                    content => {
                        warn!("Unsupported content: {:?}", to_debug(&content));
                        continue;
                    }
                };
                if message.should_reply {
                    typing_tx.send(message.session.1).await?;
                }
                user_tx.send(message).await?;
            }
        }
    }

    async fn send(
        &self,
        mut assistant_rx: mpsc::Receiver<AssistantMessage>,
        mut typing_rx: mpsc::Receiver<i64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut typing_count: HashMap<i64, usize> = HashMap::new();
        let mut typing_queue: VecDeque<(i64, Instant)> = VecDeque::new();

        loop {
            select! {
                message = assistant_rx.recv() => {
                    match message {
                        Some(message) => {
                            let session = message.session;
                            if session.0 != Platform::Telegram {
                                error!("Received mismatched platform: {:?}", session.0);
                                continue;
                            }
                            match message.content {
                                Content::Text(text) => {
                                    if let Err(e) = self.send_text(session.1, &text).await {
                                        warn!("Send text failed: {e:?}");
                                    }
                                }
                            }
                            if let hash_map::Entry::Occupied(mut entry) = typing_count.entry(session.1) {
                                *entry.get_mut() -= 1;
                                if *entry.get() == 0 {
                                    entry.remove();
                                }
                            }
                        }
                        None => return Ok(()),
                    }
                }
                chat_id = typing_rx.recv() => {
                    match chat_id {
                        Some(chat_id) => {
                            let count = typing_count.entry(chat_id).or_default();
                            *count += 1;
                            if *count == 1 {
                                typing_queue.push_front((chat_id, Instant::now()));
                            }
                        }
                        None => return Ok(()),
                    }
                }
                _ = sleep_until_or_pending(typing_queue.front().map(|(_, deadline)| deadline).copied()) => {
                    while let Some((chat_id, deadline)) = typing_queue.front().copied() {
                        let now = Instant::now();
                        if deadline > now {
                            break;
                        }
                        typing_queue.pop_front();
                        if !typing_count.contains_key(&chat_id) {
                            continue;
                        }
                        let params = SendChatActionParams::builder()
                            .chat_id(chat_id)
                            .action(ChatAction::Typing)
                            .build();
                        if let Err(e) = self.bot.send_chat_action(&params).await {
                            warn!("Send typing failed: {e:?}");
                        }
                        typing_queue.push_back((chat_id, now + Duration::from_secs(4)));
                    }
                }
            }
        }
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: use send_rich_message in a few years.
        let params = match telegram_markdown_v2::convert_with_strategy(
            text,
            UnsupportedTagsStrategy::Escape,
        ) {
            Ok(markdown) => SendMessageParams::builder()
                .chat_id(chat_id)
                .text(markdown)
                .parse_mode(ParseMode::MarkdownV2)
                .build(),
            Err(e) => {
                warn!("Markdown conversion failed: {e:?}");
                SendMessageParams::builder()
                    .chat_id(chat_id)
                    .text(text)
                    .build()
            }
        };

        self.bot.send_message(&params).await?;
        Ok(())
    }

    fn should_reply(&self, message: &frankenstein::types::Message) -> bool {
        match message.chat.type_field {
            ChatType::Private => true,
            ChatType::Group | ChatType::Supergroup => {
                self.is_reply_to_bot(message) || self.is_bot_mentioned(message)
            }
            ChatType::Channel => false,
        }
    }

    fn is_reply_to_bot(&self, message: &frankenstein::types::Message) -> bool {
        message
            .reply_to_message
            .as_ref()
            .and_then(|reply| reply.from.as_ref())
            .is_some_and(|from_user| from_user.id == self.bot_user_id)
    }

    fn is_bot_mentioned(&self, message: &frankenstein::types::Message) -> bool {
        let (Some(text), Some(entities)) = (&message.text, &message.entities) else {
            return false;
        };

        let text_utf16: Vec<u16> = text.encode_utf16().collect();
        let target: Vec<u16> = format!("@{}", self.bot_username).encode_utf16().collect();

        entities.iter().any(|entity| {
            if entity.type_field != MessageEntityType::Mention {
                return false;
            }

            let start = entity.offset as usize;
            let end = start + entity.length as usize;

            text_utf16
                .get(start..end)
                .is_some_and(|mention| eq_ignore_ascii_case(mention, &target))
        })
    }
}

fn get_user(message: &frankenstein::types::Message) -> User {
    if let Some(user) = message.from.as_deref() {
        User {
            id: format!("user_{}", user.id),
            username: user.username.clone().unwrap_or_default(),
            first_name: user.first_name.clone(),
            last_name: user.last_name.clone().unwrap_or_default(),
        }
    } else {
        let chat = message
            .sender_chat
            .as_deref()
            .unwrap_or_else(|| message.chat.as_ref());
        User {
            id: format!("chat_{}", chat.id),
            username: chat.username.clone().unwrap_or_default(),
            first_name: chat.first_name.clone().unwrap_or_default(),
            last_name: chat.last_name.clone().unwrap_or_default(),
        }
    }
}

fn eq_ignore_ascii_case(lhs: &[u16], rhs: &[u16]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|(&left, &right)| ascii_to_lowercase(left) == ascii_to_lowercase(right))
}

fn ascii_to_lowercase(value: u16) -> u16 {
    if (b'A' as u16..=b'Z' as u16).contains(&value) {
        value + (b'a' - b'A') as u16
    } else {
        value
    }
}

async fn sleep_until_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending().await,
    }
}
