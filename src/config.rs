use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub ai: AiConfig,
    pub telegram: Option<TelegramConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AiConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TelegramConfig {
    pub bot_token: String,
}
