use std::time::Duration;

use frankenstein::{
    client_reqwest::Bot,
    methods::{GetUpdatesParams, SendMessageParams},
    types::{ChatType, MessageEntityType},
    updates::UpdateContent,
    AsyncTelegramApi, ParseMode,
};
use log::{debug, error, info, warn};
use serde_fmt::to_debug;
use tokio::{select, sync::mpsc, time::sleep};

use crate::types::{AssistantMessage, Content, Platform, SessionId, User, UserMessage};

pub async fn run_bot(
    bot_token: &str,
    tx: mpsc::Sender<UserMessage>,
    rx: mpsc::Receiver<AssistantMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bot = Bot::new(bot_token);

    let bot_user = bot.get_me().await?.result;
    debug!("Got bot user: {:?}", to_debug(&bot_user));
    let bot_user_id = bot_user.id;
    let bot_username = bot_user.username.expect("The bot must have a username.");
    info!("Connected as {} ({})", bot_username, bot_user_id);

    select! {
        result = recv(bot.clone(), bot_user_id, &bot_username, tx) => result,
        result = send(bot, rx) => result,
    }
}

async fn recv(
    bot: Bot,
    bot_user_id: u64,
    bot_username: &str,
    tx: mpsc::Sender<UserMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut next_offset = None;
    loop {
        let params = match next_offset {
            Some(offset) => GetUpdatesParams::builder()
                .offset(offset)
                .timeout(30)
                .build(),
            None => GetUpdatesParams::builder().timeout(30).build(),
        };
        let updates = loop {
            match bot.get_updates(&params).await {
                Ok(response) => break response.result,
                Err(e) => {
                    warn!("Failed to get updates: {e:?}, retry after 2 seconds");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        };

        for update in updates {
            next_offset = Some(update.update_id as i64 + 1);
            let user_message = match update.content {
                UpdateContent::Message(message) => {
                    let Some(text) = &message.text else {
                        warn!("Unsupported message: {:?}", to_debug(message.as_ref()));
                        continue;
                    };
                    debug!("Received text message: {:?}", to_debug(message.as_ref()));

                    let session = SessionId(Platform::Telegram, message.chat.id);
                    let user = get_user(message.as_ref());
                    let content = Content::Text(text.clone());
                    let should_reply = should_reply(message.as_ref(), bot_user_id, bot_username);

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
            tx.send(user_message).await?;
        }
    }
}

async fn send(
    bot: Bot,
    mut rx: mpsc::Receiver<AssistantMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    while let Some(assistant_message) = rx.recv().await {
        if assistant_message.session.0 != Platform::Telegram {
            error!(
                "Received mismatched platform: {:?}",
                assistant_message.session.0
            );
            continue;
        }
        match assistant_message.content {
            Content::Text(text) => {
                if let Err(e) = bot
                    .send_message(
                        &SendMessageParams::builder()
                            .chat_id(assistant_message.session.1)
                            .text(telegram_markdown_v2::convert(&text)?)
                            .parse_mode(ParseMode::MarkdownV2)
                            .build(),
                    )
                    .await
                {
                    warn!("Failed to send message: {e:?}");
                }
            }
        }
    }
    Ok(())
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

fn should_reply(
    message: &frankenstein::types::Message,
    bot_user_id: u64,
    bot_username: &str,
) -> bool {
    match message.chat.type_field {
        ChatType::Private => true,
        ChatType::Group | ChatType::Supergroup => {
            is_reply_to_bot(message, bot_user_id) || is_bot_mentioned(message, bot_username)
        }
        ChatType::Channel => false,
    }
}

fn is_reply_to_bot(message: &frankenstein::types::Message, bot_user_id: u64) -> bool {
    message
        .reply_to_message
        .as_ref()
        .and_then(|reply| reply.from.as_ref())
        .is_some_and(|from_user| from_user.id == bot_user_id)
}

fn is_bot_mentioned(message: &frankenstein::types::Message, bot_username: &str) -> bool {
    let (Some(text), Some(entities)) = (&message.text, &message.entities) else {
        return false;
    };

    let text_utf16: Vec<u16> = text.encode_utf16().collect();
    let target: Vec<u16> = format!("@{bot_username}").encode_utf16().collect();

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
