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

use crate::types::{AssistantMessage, AssistantRequest, Content, UserMessage};

pub struct Assistant {
    client: Client<OpenAIConfig>,
    model: String,
    prompt: String,
}

#[derive(Clone, Debug)]
enum Message {
    Assistant(AssistantMessage),
    User(UserMessage),
}

impl Assistant {
    pub fn new(api_base: &str, api_key: &str, model: &str, prompt: &str) -> Self {
        let config = OpenAIConfig::new()
            .with_api_base(api_base)
            .with_api_key(api_key);
        let client = Client::with_config(config);

        Self {
            client,
            model: model.into(),
            prompt: prompt.into(),
        }
    }

    pub async fn run(
        &self,
        mut request_rx: mpsc::Receiver<AssistantRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Use persistent storage
        let mut sessions = HashMap::new();

        while let Some(request) = request_rx.recv().await {
            let session = &request.message.session;
            let messages: &mut Vec<Message> = sessions.entry(session.clone()).or_default();
            let Some(reply_tx) = request.reply_tx else {
                messages.push(Message::User(request.message));
                continue;
            };

            match self.handle_user_message(messages, &request.message).await {
                Ok(message) => {
                    if reply_tx.send(message.clone()).is_err() {
                        warn!("Send reply failed");
                    }
                    messages.extend([Message::User(request.message), Message::Assistant(message)]);
                }
                Err(e) => warn!("Handle user message failed: {e:?}"),
            }
        }
        Ok(())
    }

    async fn handle_user_message(
        &self,
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
        let request = self.to_chat_request(history, user_message)?;
        let response = self.client.chat().create(request).await?;
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
        &self,
        history: &[Message],
        user_message: &UserMessage,
    ) -> Result<CreateChatCompletionRequest, Box<dyn std::error::Error>> {
        let request_messages: Vec<ChatCompletionRequestMessage> =
            once(convert_prompt(&self.prompt))
                .chain(history.iter().map(convert_message))
                .chain(once(convert_user_message(user_message)))
                .collect::<Result<_, _>>()?;
        let request = CreateChatCompletionRequestArgs::default()
            .model(&self.model)
            .messages(request_messages)
            .build()?;
        Ok(request)
    }
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
