package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Bridge wraps a whatsmeow.Client and writes WhatsApp events into our SQLite database.
//
// Session state (whatsmeow's own sqlstore, holding encryption keys and device identity)
// lives in a separate SQLite file, both encrypted under the same SQLCipher key as the
// message database. The split follows whatsmeow's upstream pattern; we just share the key.
type Bridge struct {
	cfg         *Config
	db          *sql.DB
	client      *whatsmeow.Client
	transcriber *Transcriber

	// rootCtx is the process-lifetime context, kept so event handlers can
	// restart the login loop (e.g. after a WhatsApp-side logout) without
	// holding a request-scoped ctx.
	rootCtx context.Context

	mu            sync.RWMutex
	connected     bool
	authenticated bool
	deviceJID     string
	lastSyncTime  time.Time

	// Auth lifecycle surfaced over /api/status + /api/auth/* (see auth.go).
	authState        AuthState
	currentQR        string
	qrExpiresAt      time.Time
	pairingCode      string
	loggedOutReason  string
	loginRunning     bool
	pairPhoneOnStart string // --pair-phone flag: request a typed code on first QR event
}

// NewBridge builds the whatsmeow client, prepares its session store, and registers event handlers.
// It does NOT connect yet; call Connect().
// If transcriber is non-nil, voice-note messages are enqueued for transcription automatically.
func NewBridge(ctx context.Context, cfg *Config, db *sql.DB, dbKey string, transcriber *Transcriber) (*Bridge, error) {
	sessionDir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir session dir: %w", err)
	}
	sessionPath := filepath.Join(sessionDir, "session.db")

	dsn := fmt.Sprintf("file:%s?_foreign_keys=on", sessionPath)
	if cfg.EncryptDB {
		dsn = fmt.Sprintf("file:%s?_pragma_key=x'%s'&_pragma_cipher_page_size=4096&_foreign_keys=on",
			sessionPath, dbKey)
	}

	dbLog := waLog.Stdout("whatsmeow-store", "WARN", true)
	container, err := sqlstore.New(ctx, "sqlite3", dsn, dbLog)
	if err != nil {
		return nil, fmt.Errorf("whatsmeow sqlstore init: %w", err)
	}

	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("get first device: %w", err)
	}

	clientLog := waLog.Stdout("whatsmeow-client", "WARN", true)
	client := whatsmeow.NewClient(device, clientLog)

	b := &Bridge{
		cfg:         cfg,
		db:          db,
		client:      client,
		transcriber: transcriber,
		rootCtx:     ctx,
		authState:   AuthStateUnauthenticated,
	}
	client.AddEventHandler(b.handleEvent)
	return b, nil
}

// Authentication (QR loop, pairing codes, re-login) lives in auth.go —
// main() runs RunAuth in a goroutine so the HTTP API is available during
// first-run pairing.

func (b *Bridge) Disconnect() {
	if b.client != nil {
		b.client.Disconnect()
	}
	b.mu.Lock()
	b.connected = false
	b.authenticated = false
	b.mu.Unlock()
}

// Status returns current connection/auth state for the /api/status handler.
func (b *Bridge) Status() (connected, authed bool, deviceJID string, lastSync int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var ls int64
	if !b.lastSyncTime.IsZero() {
		ls = b.lastSyncTime.Unix()
	}
	return b.connected, b.authenticated, b.deviceJID, ls
}

func (b *Bridge) DeviceJID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.deviceJID
}

// Client returns the underlying whatsmeow client. Used by the backfiller
// to call client.Download against reconstituted AudioMessages. Read-only
// in spirit, but the whatsmeow client itself is not.
func (b *Bridge) Client() *whatsmeow.Client {
	return b.client
}

// IsConnected returns true when the bridge has an active whatsmeow connection
// and an authenticated device. Used by the send flow to refuse confirmations
// when the bridge is offline.
func (b *Bridge) IsConnected() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connected && b.authenticated
}

// --- Event dispatch --------------------------------------------------------

func (b *Bridge) handleEvent(raw interface{}) {
	switch evt := raw.(type) {
	case *events.Message:
		b.onMessage(evt)
	case *events.Receipt:
		b.onReceipt(evt)
	case *events.Connected:
		b.mu.Lock()
		b.connected = true
		if b.authenticated {
			b.authState = AuthStatePaired
		}
		b.mu.Unlock()
		log.Println("whatsmeow: connected")
	case *events.Disconnected:
		b.mu.Lock()
		b.connected = false
		b.mu.Unlock()
		log.Println("whatsmeow: disconnected")
	case *events.LoggedOut:
		b.mu.Lock()
		b.connected = false
		b.authenticated = false
		b.authState = AuthStateLoggedOut
		b.loggedOutReason = fmt.Sprintf("%v", evt.Reason)
		b.currentQR = ""
		b.pairingCode = ""
		b.mu.Unlock()
		log.Printf("whatsmeow: logged out; reason=%v — starting a fresh pairing flow (scan the new QR, or POST /api/auth/pair-phone)", evt.Reason)
		// Re-enter pairing without a process restart. whatsmeow clears the
		// device store on logout; the small delay lets that settle.
		go func() {
			select {
			case <-b.rootCtx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			if err := b.loginLoop(b.rootCtx); err != nil && b.rootCtx.Err() == nil {
				log.Printf("re-login after logout failed: %v (POST /api/auth/reconnect to retry)", err)
			}
		}()
	case *events.PairSuccess:
		b.mu.Lock()
		b.authenticated = true
		b.authState = AuthStatePaired
		b.currentQR = ""
		b.pairingCode = ""
		b.loggedOutReason = ""
		if evt.ID.String() != "" {
			b.deviceJID = evt.ID.String()
		}
		b.mu.Unlock()
		log.Printf("whatsmeow: paired successfully; device=%s", evt.ID.String())
	case *events.HistorySync:
		b.mu.Lock()
		b.lastSyncTime = time.Now()
		b.mu.Unlock()
		log.Printf("whatsmeow: history sync chunk (progress=%d, conversations=%d)",
			evt.Data.GetProgress(), len(evt.Data.GetConversations()))
		// Backfill media-key fields for any historical image/video/document/
		// sticker messages WhatsApp re-delivers. See history_sync.go.
		b.processHistorySyncEvent(evt)
	case *events.CallOffer:
		b.onCallOffer(evt)
	case *events.CallTerminate:
		b.onCallTerminate(evt)
	}
}

// onMessage persists an incoming or outgoing message.
// The original content text is stored as-is; the prompt-injection-scrubbed representation
// is stored in scrubbed_text (plus flags in scrub_flags_json) for Claude to consume.
func (b *Bridge) onMessage(evt *events.Message) {
	chatJID := evt.Info.Chat.String()
	senderJID := evt.Info.Sender.String()
	id := evt.Info.ID
	ts := evt.Info.Timestamp.Unix()

	msg := unwrapMessage(evt.Message)
	content, msgType := extractContent(msg)
	if msgType == "system" {
		// Field names only (no content); raw shows the pre-UnwrapRaw wrappers.
		log.Printf("onMessage: unhandled message shape id=%s chat=%s fields=%v raw=%v",
			id, chatJID, messageFieldNames(msg), messageFieldNames(evt.RawMessage))
	}
	normalized := Normalize(content)
	scrubbed, flags := Scrub(content)

	senderDisplay := evt.Info.PushName
	if senderDisplay == "" {
		senderDisplay = evt.Info.Sender.User
	}

	// Upsert chat row (minimal; full chat sync happens via HistorySync).
	_, err := b.db.Exec(`
		INSERT INTO chats (jid, chat_type, name, normalized_name, created_at, updated_at, last_message_id, last_message_time, last_message_preview)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			last_message_id = excluded.last_message_id,
			last_message_time = excluded.last_message_time,
			last_message_preview = excluded.last_message_preview,
			updated_at = excluded.updated_at
	`, chatJID, chatTypeFromJID(evt.Info.Chat), senderDisplay, Normalize(senderDisplay),
		ts, ts, id, ts, truncate(content, 120))
	if err != nil {
		log.Printf("onMessage: chat upsert failed: %v", err)
	}

	// Capture re-download fields up front for any media-bearing message
	// (image/video/document/audio/sticker), so we can refetch later via
	// whatsmeow.Client.Download. Without these, image/document content is
	// only available at the moment of receipt — any consumer that wakes up
	// later (receipts pipeline, vision OCR, vault export with attachments)
	// has no way to recover the bytes. See media_download.go.
	mfields, _ := extractFromMessage(msg)

	// Insert message. Media columns are populated for image/video/document/
	// audio/sticker; for text/system/reaction etc. they go in as NULL.
	_, err = b.db.Exec(`
		INSERT INTO messages (id, chat_jid, sender_jid, sender_display, timestamp, type, content_text, content_normalized, is_from_me, scrubbed_text, scrub_flags_json,
			media_key, media_direct_path, media_url, media_enc_sha256, media_sha256, media_file_length, media_key_timestamp, media_mime)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, id, chatJID, senderJID, senderDisplay, ts, msgType, content, normalized,
		boolToInt(evt.Info.IsFromMe), scrubbed, ScrubFlagsJSON(flags),
		mfields.MediaKey, mfields.MediaDirectPath, mfields.MediaURL,
		mfields.MediaEncSHA, mfields.MediaSHA, mfields.MediaFileLength,
		mfields.MediaKeyTimestamp, mfields.MediaMime)
	if err != nil {
		log.Printf("onMessage: message insert failed: %v", err)
	}

	// Upsert contact row for the sender (non-group messages; group participants sync separately).
	//
	// Phone-column rule: only store `evt.Info.Sender.User` as phone when the
	// JID is the @s.whatsapp.net (DefaultUserServer) form, where User IS a
	// phone number. For @lid (HiddenUserServer) JIDs, User is an opaque LID
	// number, NOT a phone. Storing it as phone produces the bug class where
	// search and CRM lookups by phone silently miss the LID-form row. Prefer
	// SenderAlt when it's the phone form; otherwise leave phone NULL and let
	// the startup BackfillJIDAliases repair it once whatsmeow's LID store
	// learns the mapping.
	if !evt.Info.IsGroup {
		var phoneToStore sql.NullString
		switch {
		case evt.Info.Sender.Server == types.DefaultUserServer:
			phoneToStore = sql.NullString{String: evt.Info.Sender.User, Valid: true}
		case !evt.Info.SenderAlt.IsEmpty() && evt.Info.SenderAlt.Server == types.DefaultUserServer:
			phoneToStore = sql.NullString{String: evt.Info.SenderAlt.User, Valid: true}
		}
		_, err = b.db.Exec(`
			INSERT INTO contacts (jid, phone, push_name, normalized_name, is_business, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(jid) DO UPDATE SET
				push_name = excluded.push_name,
				normalized_name = excluded.normalized_name,
				phone = COALESCE(excluded.phone, contacts.phone),
				updated_at = excluded.updated_at
		`, senderJID, phoneToStore, senderDisplay, Normalize(senderDisplay), 0, ts, ts)
		if err != nil {
			log.Printf("onMessage: contact upsert failed: %v", err)
		}

		// Record JID alias edges. WhatsApp now exposes most contacts as a
		// (LID, phone-JID) pair; whatsmeow surfaces the alt form via
		// SenderAlt for incoming DMs and RecipientAlt for outgoing. Without
		// this, the two forms accumulate as orphaned rows and search/list
		// returns whichever the bridge happened to see first. See aliases.go.
		ctx := context.Background()
		if evt.Info.IsFromMe {
			recordJIDAlias(ctx, b.db, evt.Info.Chat, evt.Info.RecipientAlt, "message_recipient", ts)
		} else {
			recordJIDAlias(ctx, b.db, evt.Info.Sender, evt.Info.SenderAlt, "message_sender", ts)
		}
	}

	// Enqueue voice-note transcription. Fire-and-forget; the transcriber
	// writes back to messages.voice_note_transcript when done.
	if b.transcriber != nil && (msgType == "voice" || msgType == "audio") {
		if audio := msg.GetAudioMessage(); audio != nil {
			audioMsg := audio // closure capture
			client := b.client
			b.transcriber.Enqueue(transcriptionJob{
				MessageID: id,
				ChatJID:   chatJID,
				MimeType:  audio.GetMimetype(),
				AudioDownloader: func(ctx context.Context) ([]byte, error) {
					return client.Download(ctx, audioMsg)
				},
			})
		}
	}
}

func (b *Bridge) onReceipt(evt *events.Receipt) {
	// Receipts are informational for now; we log them but don't alter state.
	// Future: update read-status on message rows if useful for list_messages output.
	_ = evt
}

func (b *Bridge) onCallOffer(evt *events.CallOffer) {
	if !b.cfg.CaptureCalls {
		return
	}
	chatJID := evt.CallCreator.String()
	_, err := b.db.Exec(`
		INSERT INTO calls (id, chat_jid, caller_jid, timestamp, call_type, is_group, is_outbound, result)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO NOTHING
	`, evt.CallID, chatJID, evt.CallCreator.String(), evt.Timestamp.Unix(), "voice", 0, 0, "offered")
	if err != nil {
		log.Printf("onCallOffer: insert failed: %v", err)
	}
}

func (b *Bridge) onCallTerminate(evt *events.CallTerminate) {
	if !b.cfg.CaptureCalls {
		return
	}
	result := strings.ToLower(string(evt.Reason))
	if result == "" {
		result = "ended"
	}
	_, err := b.db.Exec(`
		UPDATE calls SET result = ? WHERE id = ?
	`, result, evt.CallID)
	if err != nil {
		log.Printf("onCallTerminate: update failed: %v", err)
	}
}

// --- Helpers ---------------------------------------------------------------

func chatTypeFromJID(j types.JID) string {
	switch j.Server {
	case types.GroupServer:
		return "group"
	case types.BroadcastServer:
		return "broadcast"
	case types.NewsletterServer:
		return "broadcast"
	default:
		return "direct"
	}
}

var futureProofName = (&waE2E.FutureProofMessage{}).ProtoReflect().Descriptor().FullName()

// maxUnwrapDepth bounds wrapper recursion; real messages nest 2-3 deep.
const maxUnwrapDepth = 8

// unwrapMessage peels every wrapper whatsmeow's UnwrapRaw leaves in place
// (associatedChildMessage for album items, groupMentionedMessage,
// spoilerMessage, botForwardedMessage, ...) by descending into any set
// FutureProofMessage or DeviceSentMessage field. Generic on purpose:
// WhatsApp keeps adding FutureProofMessage wrappers, and a hard-coded list
// is how album documents ended up stored as "system" with no media key.
// History-sync protos are never unwrapped by whatsmeow, so this also
// covers documentWithCaption/ephemeral/viewOnce there.
func unwrapMessage(m *waE2E.Message) *waE2E.Message {
	for depth := 0; m != nil && depth < maxUnwrapDepth; depth++ {
		inner := m.GetDeviceSentMessage().GetMessage()
		if inner == nil {
			m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				if fd.IsList() || fd.Message() == nil || fd.Message().FullName() != futureProofName {
					return true
				}
				inner = v.Message().Interface().(*waE2E.FutureProofMessage).GetMessage()
				return inner == nil
			})
		}
		if inner == nil {
			return m
		}
		m = inner
	}
	return m
}

// messageFieldNames lists the proto field names set on m — type names only,
// never values — so unhandled message shapes can be diagnosed from the log.
func messageFieldNames(m *waE2E.Message) []string {
	var names []string
	if m == nil {
		return names
	}
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		names = append(names, string(fd.Name()))
		return true
	})
	return names
}

// extractContent expects an already-unwrapped message (see unwrapMessage).
func extractContent(m *waE2E.Message) (text, msgType string) {
	switch {
	case m.GetConversation() != "":
		return m.GetConversation(), "text"
	case m.GetExtendedTextMessage() != nil:
		return m.GetExtendedTextMessage().GetText(), "text"
	case m.GetImageMessage() != nil:
		return m.GetImageMessage().GetCaption(), "image"
	case m.GetVideoMessage() != nil:
		return m.GetVideoMessage().GetCaption(), "video"
	case m.GetAudioMessage() != nil:
		if m.GetAudioMessage().GetPTT() {
			return "", "voice"
		}
		return "", "audio"
	case m.GetDocumentMessage() != nil:
		return m.GetDocumentMessage().GetCaption(), "document"
	case m.GetStickerMessage() != nil:
		return "", "sticker"
	case m.GetLocationMessage() != nil:
		return m.GetLocationMessage().GetComment(), "location"
	case m.GetContactMessage() != nil:
		return m.GetContactMessage().GetDisplayName(), "contact"
	case m.GetReactionMessage() != nil:
		return m.GetReactionMessage().GetText(), "reaction"
	default:
		return "", "system"
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
