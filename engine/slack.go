package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type SlackOpts struct {
	Channel    string `aql:"SLACK_CHANNEL, optional"`
	Emoji      string `aql:"SLACK_EMOJI, optional"`
	Username   string `aql:"SLACK_USER, optional"`
	WebhookURL string `aql:"SLACK_WEBHOOK_URL"`
	MinLevel   string `aql:"SLACK_LOG_LEVEL"`
	Script     string `aql:"SLACK_NAME, optional"`
}

type slackLogger struct {
	Opts     SlackOpts
	l        Logger
	minLevel LogLevel
	c        chan Event
	client   *http.Client
}

type slackPayload struct {
	Text     string `json:"text"`
	Channel  string `json:"channel, omitempty"`
	Username string `json:"username, omitempty"`
	Emoji    string `json:"icon_emoji, omitempty"`
}

func (s *slackLogger) Chan() chan<- Event {
	return s.c
}

func (s *slackLogger) sendSlackMessage(msg Event, errChan chan<- Event) {
	payload := slackPayload{
		Text:     fmt.Sprintf("<%s>: %s - %s - %s", s.Opts.Script, msg.Source, eventTypeMap[msg.Level], msg.Message),
		Channel:  s.Opts.Channel,
		Username: s.Opts.Username,
		Emoji:    s.Opts.Emoji,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		if err != nil {
			errChan <- Event{
				Level:   Warning,
				Source:  "Slack",
				Time:    time.Now(),
				Message: fmt.Sprintf("Error marshalling message for Slack: %v", err),
			}
		}
	}
	body := bytes.NewBuffer(data)
	request, err := http.NewRequest("POST", s.Opts.WebhookURL, body)
	if err != nil {
		if err != nil {
			errChan <- Event{
				Level:   Warning,
				Source:  "Slack",
				Time:    time.Now(),
				Message: fmt.Sprintf("Error sending message to Slack: %v", err),
			}
		}
	}
	request.Header.Add("Content-Type", "application/json; charset=utf-8")
	_, err = s.client.Do(request)
	if err != nil {
		errChan <- Event{
			Level:   Warning,
			Source:  "Slack",
			Time:    time.Now(),
			Message: fmt.Sprintf("Error sending message to Slack: %v", err),
		}
	}
}

func StrToLevel(s string) (LogLevel, bool) {
	switch strings.ToLower(s) {
	case "trace":
		return Trace, true
	case "info":
		return Info, true
	case "warning":
		return Warning, true
	case "error":
		return Error, true
	}
	return Error, false
}

//SlackWrapper intercepts messages to a logger and forwards any with the given minimum log level to Slack incoming Webhook.
func SlackWrapper(l Logger, opts SlackOpts) Logger {
	if opts.WebhookURL == "" {
		panic("blank webhook URL")
	}
	min, ok := StrToLevel(opts.MinLevel)
	if !ok {
		panic(fmt.Sprintf("invalid level %s", opts.MinLevel))
	}
	if opts.Script == "" {
		opts.Script = "Unnamed script"
	}
	s := slackLogger{
		Opts:     opts,
		l:        l,
		minLevel: min,
		c:        make(chan Event, DefaultBufferSize),
		client:   &http.Client{},
	}
	outChan := l.Chan()
	go func() {
		for msg := range s.c {
			outChan <- msg
			if msg.Level >= min {
				go s.sendSlackMessage(msg, outChan)
			}
		}
	}()
	return &s
}
