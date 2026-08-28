# unai

unai (You & AI) is an AI prompt serving system.

## Usage

```bash
# Ask a question
unai --config unai.toml --prompt examples/prompt.md ask "Who are you?"

# Run interactive chat
unai --config unai.toml --prompt examples/prompt.md chat

# Run Telegram bot
unai --config unai.toml --prompt examples/prompt.md telegram-bot
```

Example config:

```toml
[ai]
api_base = "https://anypal.ai/api/openai"
api_key = "sk-..."
model = "gpt-5.6-terra"

# Required only for the `telegram-bot` command.
[telegram]
bot_token = "..."
```

## Acknowledgments

Thanks [Yuko](https://yuko.me) for the AnyPal software and AI inference infrastructure.

## License

This project is licensed under the MIT NON-AI License. See the LICENSE file for details.
