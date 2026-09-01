use tokio::sync::oneshot;

pub struct AssistantRequest {
    pub message: UserMessage,
    pub reply_tx: Option<oneshot::Sender<AssistantMessage>>,
}

#[derive(Clone, Debug)]
pub struct UserMessage {
    pub session: SessionId,
    pub user: User,
    pub content: Content,
    // TODO: time
}

#[derive(Clone, Debug)]
pub struct AssistantMessage {
    pub session: SessionId,
    pub content: Content,
    // TODO: time
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Platform {
    Local,
    Telegram,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SessionId(pub Platform, pub i64);

#[derive(Clone, Debug, Default)]
pub struct User {
    pub id: String,
    pub username: String,
    pub first_name: String,
    pub last_name: String,
}

#[derive(Clone, Debug)]
pub enum Content {
    Text(String),
}
