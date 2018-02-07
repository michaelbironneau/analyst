package engine

import (
	"time"
	"github.com/keighl/mandrill"
	"fmt"
	"sync/atomic"
)


type MandrillPrincipal struct {
	Name string
	Email string
}

type MandrillDestination struct {
	Name string
	APIKey string
	Sender *MandrillPrincipal
	Recipients []MandrillPrincipal
	SplitByRow bool
	Template string
	Subject string
	client *mandrill.Client
	emailsSent int64
	cols []string
}

func (d *MandrillDestination) Ping() error {
	d.client = mandrill.ClientWithKey(d.APIKey)
	_, err := d.client.Ping()
	return err
}
func (d *MandrillDestination) Open(s Stream, l Logger, st Stopper) {
	c := s.Chan(d.Name)
	var (
		rows []map[string]interface{}
		cols []string
		firstMessage = true
	)

	for msg := range c {
		d.log(l, Info, "Mandrill destination opened")
		if st.Stopped() {
			d.log(l, Warning, "Mandrill destination aborted")
			return
		}
		if firstMessage {
			cols = s.Columns()
			firstMessage = false
		}

		if d.SplitByRow {
			m := d.prepareMsg()
			content := d.prepareContent(cols, msg.Data)
			_, err := d.client.MessagesSendTemplate(m, d.Template, content)
			atomic.AddInt64(&d.emailsSent, 1)
			if err != nil {
				d.fatalerr(err, l)
				return
			}
			d.log(l, Trace, "sent email to recipients with content %v", content)
		} else {
			rows = append(rows, d.prepareContent(cols, msg.Data))
		}
	}

	if !d.SplitByRow {
		m := d.prepareMsg()
		_, err := d.client.MessagesSendTemplate(m, d.Template, rows)
		atomic.AddInt64(&d.emailsSent, 1)
		if err != nil {
			d.fatalerr(err, l)
			return
		}
		d.log(l, Info, "sent email to recipients containing all received rows")
	}
	d.log(l, Info, "Sent a total of %v emails - finished", d.emailsSent)
}

func (d *MandrillDestination) prepareContent(cols []string, row []interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for i, col := range cols {
		ret[col] = row[i]
	}
	return ret
}

func (d *MandrillDestination) prepareMsg() *mandrill.Message{
	var m mandrill.Message
	if d.Subject != "" {
		//Could be set as part of the template
		m.Subject = d.Subject
	}
	if d.Sender != nil {
		//Could be set as part of the template
		m.FromName = d.Sender.Name
		m.FromEmail = d.Sender.Email
	}
	for _, recipient := range d.Recipients {
		m.AddRecipient(recipient.Email, recipient.Name, "to")
	}
	return &m
}



func (d *MandrillDestination) log(l Logger, level LogLevel, msgFormat string, args...interface{}) {
	l.Chan() <- Event{
		Source:  d.Name,
		Level:   level,
		Time:    time.Now(),
		Message: fmt.Sprintf(msgFormat, args...),
	}
}

func (d *MandrillDestination) fatalerr(err error, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  d.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
}
