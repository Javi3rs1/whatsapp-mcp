package main

import (
	"database/sql"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func testPDF() *waE2E.Message {
	return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		MediaKey:   []byte{1, 2, 3},
		DirectPath: proto.String("/v/t62/doc"),
		Mimetype:   proto.String("application/pdf"),
		Caption:    proto.String("factura"),
	}}
}

// wrap nests m inside a FutureProofMessage set by setter.
func wrap(m *waE2E.Message, setter func(*waE2E.Message, *waE2E.FutureProofMessage)) *waE2E.Message {
	outer := &waE2E.Message{MessageContextInfo: &waE2E.MessageContextInfo{}}
	setter(outer, &waE2E.FutureProofMessage{Message: m})
	return outer
}

// Regression: album items arrive as associatedChildMessage, which whatsmeow's
// UnwrapRaw does not peel, so documents were stored as "system" with no key.
func TestUnwrappedMediaIsClassifiedAndKeyed(t *testing.T) {
	cases := []struct {
		name string
		msg  *waE2E.Message
	}{
		{"plain", testPDF()},
		{"associatedChild", wrap(testPDF(), func(o *waE2E.Message, f *waE2E.FutureProofMessage) { o.AssociatedChildMessage = f })},
		{"groupMentioned", wrap(testPDF(), func(o *waE2E.Message, f *waE2E.FutureProofMessage) { o.GroupMentionedMessage = f })},
		// History sync is never unwrapped by whatsmeow; nested wrappers must peel fully.
		{"ephemeral>documentWithCaption", wrap(
			wrap(testPDF(), func(o *waE2E.Message, f *waE2E.FutureProofMessage) { o.DocumentWithCaptionMessage = f }),
			func(o *waE2E.Message, f *waE2E.FutureProofMessage) { o.EphemeralMessage = f })},
		{"deviceSent", &waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{Message: testPDF()}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			text, typ := extractContent(unwrapMessage(c.msg))
			if typ != "document" || text != "factura" {
				t.Errorf("extractContent = (%q, %q), want (factura, document)", text, typ)
			}
			f, ok := extractFromMessage(c.msg)
			if !ok || string(f.MediaKey) != "\x01\x02\x03" || f.MediaMime.String != "application/pdf" {
				t.Errorf("extractFromMessage = %+v, %v; want media key + pdf mime", f, ok)
			}
		})
	}
}

func TestUnwrapLeavesNonWrappedAlone(t *testing.T) {
	m := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{}, MessageContextInfo: &waE2E.MessageContextInfo{}}
	if got := unwrapMessage(m); got != m {
		t.Fatal("unwrapMessage changed a non-wrapped message")
	}
	if _, typ := extractContent(m); typ != "system" {
		t.Errorf("protocol message type = %q, want system", typ)
	}
	if got := messageFieldNames(m); len(got) != 2 {
		t.Errorf("messageFieldNames = %v, want 2 field names", got)
	}
	if unwrapMessage(nil) != nil {
		t.Error("unwrapMessage(nil) != nil")
	}
}

// Existing rows stored as "system" with no key must be repaired by history
// sync: key backfilled AND type fixed, otherwise /api/media/download still
// refuses them ("system" is not downloadable).
func TestHistorySyncRepairsSystemRows(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := applyMigrations(db); err != nil {
		t.Fatal(err)
	}
	rows := []struct{ id, typ, text string }{{"SYS1", "system", ""}, {"TXT1", "text", "keep me"}}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO messages (id, chat_jid, timestamp, type, content_text) VALUES (?, 'g@g.us', 1, ?, ?)`,
			r.id, r.typ, r.text); err != nil {
			t.Fatal(err)
		}
	}

	child := wrap(testPDF(), func(o *waE2E.Message, f *waE2E.FutureProofMessage) { o.AssociatedChildMessage = f })
	hsMsg := func(id string) *waHistorySync.HistorySyncMsg {
		return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{ID: proto.String(id)}, Message: child,
		}}
	}
	b := &Bridge{db: db}
	b.processHistorySyncEvent(&events.HistorySync{Data: &waHistorySync.HistorySync{
		Conversations: []*waHistorySync.Conversation{{ID: proto.String("g@g.us"),
			Messages: []*waHistorySync.HistorySyncMsg{hsMsg("SYS1"), hsMsg("TXT1")}}},
	}})

	var typ, text string
	var key []byte
	if err := db.QueryRow(`SELECT type, content_text, media_key FROM messages WHERE id='SYS1'`).Scan(&typ, &text, &key); err != nil {
		t.Fatal(err)
	}
	if typ != "document" || text != "factura" || len(key) == 0 {
		t.Errorf("SYS1 = (%q, %q, key=%d bytes), want (document, factura, key)", typ, text, len(key))
	}
	// Non-system rows keep their type and text; only NULL media fields fill in.
	if err := db.QueryRow(`SELECT type, content_text FROM messages WHERE id='TXT1'`).Scan(&typ, &text); err != nil {
		t.Fatal(err)
	}
	if typ != "text" || text != "keep me" {
		t.Errorf("TXT1 = (%q, %q), want unchanged (text, keep me)", typ, text)
	}
}
