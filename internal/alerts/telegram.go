package alerts

import (
	"context"
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/htb/htb-replicator/config"
)

// telegramAlerter sends alerts via the Telegram Bot API.
type telegramAlerter struct {
	bot    *tgbotapi.BotAPI
	chatID int64
}

// NewTelegramAlerter creates a new Telegram alerter using the provided config.
func NewTelegramAlerter(cfg config.TelegramConfig) (Alerter, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("telegram: token is required")
	}
	if cfg.ChatID == 0 {
		return nil, fmt.Errorf("telegram: chat_id is required")
	}

	bot, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("telegram: create bot: %w", err)
	}

	return &telegramAlerter{
		bot:    bot,
		chatID: cfg.ChatID,
	}, nil
}

// Send dispatches the alert as a Telegram message with MarkdownV2 formatting.
func (t *telegramAlerter) Send(ctx context.Context, alert Alert) error {
	emoji := levelEmoji(alert.Level)
	text := fmt.Sprintf("%s *S3 Replicator Alert*\n\n*Level:* %s\n*Destination:* `%s`\n*Message:* %s",
		emoji,
		escapeMarkdown(alert.Level),
		escapeMarkdown(alert.Destination),
		escapeMarkdown(alert.Message),
	)
	if alert.ObjectKey != "" {
		text += fmt.Sprintf("\n*Object:* `%s`", escapeMarkdown(alert.ObjectKey))
	}
	if alert.Error != nil {
		text += fmt.Sprintf("\n*Error:* `%s`", escapeMarkdown(alert.Error.Error()))
	}

	msg := tgbotapi.NewMessage(t.chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdownV2

	if _, err := t.bot.Send(msg); err != nil {
		return fmt.Errorf("telegram send: %w", err)
	}
	return nil
}

// Close is a no-op for the Telegram alerter.
func (t *telegramAlerter) Close() error { return nil }

func levelEmoji(level string) string {
	switch level {
	case "error":
		return "🔴"
	case "warning":
		return "🟡"
	case "info":
		return "🟢"
	default:
		return "⚪"
	}
}

// escapeMarkdown escapes special MarkdownV2 characters as required by Telegram.
func escapeMarkdown(s string) string {
	special := `\_*[]()~` + "`" + `>#+-=|{}.!`
	var result []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		for j := 0; j < len(special); j++ {
			if c == special[j] {
				result = append(result, '\\')
				break
			}
		}
		result = append(result, c)
	}
	return string(result)
}
