use std::{collections::HashMap, io, iter::once};

use async_openai::{
    config::OpenAIConfig,
    types::chat::{
        ChatCompletionRequestAssistantMessage, ChatCompletionRequestMessage,
        ChatCompletionRequestSystemMessage, ChatCompletionRequestUserMessage,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    },
    Client,
};
use log::{debug, info, warn};
use serde_fmt::to_debug;
use tokio::sync::mpsc;

use crate::types::{AssistantMessage, Content, UserMessage};

#[derive(Clone, Debug)]
enum Message {
    Assistant(AssistantMessage),
    User(UserMessage),
}

pub async fn run(
    api_base: &str,
    api_key: &str,
    model: &str,
    prompt: &str,
    tx: mpsc::Sender<AssistantMessage>,
    mut rx: mpsc::Receiver<UserMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = OpenAIConfig::new()
        .with_api_base(api_base)
        .with_api_key(api_key);
    let client = Client::with_config(config);

    // TODO: Use persistent storage
    let mut sessions = HashMap::new();

    while let Some(user_message) = rx.recv().await {
        let messages: &mut Vec<Message> = sessions.entry(user_message.session.clone()).or_default();
        if !user_message.should_reply {
            messages.push(Message::User(user_message));
            continue;
        }
        match handle_user_message(&client, model, prompt, messages, &user_message).await {
            Ok(assistant_message) => {
                tx.send(assistant_message.clone()).await?;
                messages.extend([
                    Message::User(user_message),
                    Message::Assistant(assistant_message),
                ]);
            }
            Err(e) => warn!("Failed to handle user message: {e:?}"),
        }
    }
    Ok(())
}

async fn handle_user_message(
    client: &Client<OpenAIConfig>,
    model: &str,
    prompt: &str,
    history: &[Message],
    user_message: &UserMessage,
) -> Result<AssistantMessage, Box<dyn std::error::Error>> {
    // TODO: Log input messages and token count
    debug!(
        "Input: [{} {} ({})] {:?}",
        user_message.user.first_name,
        user_message.user.last_name,
        user_message.user.username,
        user_message.content
    );
    let request = to_chat_request(model, prompt, history, user_message)?;
    let response = client.chat().create(request).await?;
    match &response.usage {
        Some(usage) => info!("Usage: {:?}", to_debug(usage)),
        None => warn!("Response missing usage data."),
    }
    let choice = response.choices.first().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "chat completion returned empty choices",
        )
    })?;
    let content = choice.message.content.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "chat completion returned empty content",
        )
    })?;
    debug!("Output: {}", content);
    let assistant_message = AssistantMessage {
        session: user_message.session.clone(),
        content: Content::Text(content.clone()),
    };
    Ok(assistant_message)
}

fn to_chat_request(
    model: &str,
    prompt: &str,
    history: &[Message],
    user_message: &UserMessage,
) -> Result<CreateChatCompletionRequest, Box<dyn std::error::Error>> {
    let request_messages: Vec<ChatCompletionRequestMessage> = once(convert_prompt(prompt))
        .chain(history.iter().map(convert_message))
        .chain(once(convert_user_message(user_message)))
        .collect::<Result<_, _>>()?;
    let request = CreateChatCompletionRequestArgs::default()
        .model(model)
        .messages(request_messages)
        .build()?;
    Ok(request)
}

fn convert_prompt(
    prompt: &str,
) -> Result<ChatCompletionRequestMessage, Box<dyn std::error::Error>> {
    Ok(ChatCompletionRequestSystemMessage::from(prompt).into())
}

fn convert_message(
    message: &Message,
) -> Result<ChatCompletionRequestMessage, Box<dyn std::error::Error>> {
    match message {
        Message::Assistant(assistant_message) => {
            let Content::Text(content) = &assistant_message.content;
            Ok(ChatCompletionRequestAssistantMessage::from(content.as_str()).into())
        }
        Message::User(user_message) => convert_user_message(user_message),
    }
}

fn convert_user_message(
    user_message: &UserMessage,
) -> Result<ChatCompletionRequestMessage, Box<dyn std::error::Error>> {
    let Content::Text(content) = &user_message.content;
    Ok(ChatCompletionRequestUserMessage {
        content: content.as_str().into(),
        name: Some(sanitize_openai_name(&user_message.user.id)),
    }
    .into())
}

fn sanitize_openai_name(name: &str) -> String {
    const MAX_LEN: usize = 64;
    const REPLACEMENT: char = '_';

    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                REPLACEMENT
            }
        })
        .take(MAX_LEN)
        .collect()
}
