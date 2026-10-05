// history_sync.go — programmatic history-sync backfill for media keys.
//
// Background: the bridge originally lost media-key fields for image / video /
// document / sticker messages received before the 2026-05-08 patch (it only
// persisted them for audio). The follow-up patch fixes the live receive path,
// but does NOT recover historical messages that already exist in the messages
// table with NULL media-key fields.
//
// This file fixes the historical gap programmatically. WhatsApp's history-sync
// protocol delivers the same waE2E.Message protos that arrive via live receive,
// inside HistorySyncMsg wrappers attached to Conversation entries. Processing
// those events lets us backfill media-key fields without flipping the global
// AutoDownloadMedia flag, without asking the user to manually export a chat,
// and without storing duplicate media bytes anywhere.
//
// Two halves:
//
//   1. processHistorySyncEvent — invoked from the live event handler when
//      whatsmeow delivers a *events.HistorySync. Walks every Conversation's
//      messages, extracts media-key fields, and runs an UPDATE that only
//      writes when the existing row's media_key is NULL (so we don't clobber
//      good values with stale ones).
//
//   2. RequestChatHistory — finds the oldest message we have for a chat and
//      sends a peer HistorySyncOnDemandRequest asking WhatsApp for `count`
//      messages immediately before that one. WhatsApp delivers the response
//      asynchronously over the events.HistorySync stream, which #1 picks up.
//
// Net effect: a single POST /api/admin/request-history call against a chat
// triggers the backfill, and the receipts pipeline (or any other consumer)
// can immediately decrypt the historical media via the existing
// /api/media/download endpoint.

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// extractFromMessage is the inner-type variant of extractDownloadableFields.
// Live receive (events.Message) wraps the proto in evt.Message; HistorySync
// delivery wraps it in HistorySyncMsg.GetMessage().GetMessage(). Both reach
// the same *waE2E.Message, so both paths share this helper. Wrappers are
// peeled here because history-sync protos arrive un-unwrapped.
func extractFromMessage(m *waE2E.Message) (mediaFields, bool) {
	m = unwrapMessage(m)
	if m == nil {
		return mediaFields{}, false
	}
	if mm := m.GetImageMessage(); mm != nil {
		return fieldsFrom(mm), true
	}
	if mm := m.GetVideoMessage(); mm != nil {
		return fieldsFrom(mm), true
	}
	if mm := m.GetDocumentMessage(); mm != nil {
		return fieldsFrom(mm), true
	}
	if mm := m.GetAudioMessage(); mm != nil {
		return fieldsFrom(mm), true
	}
	if mm := m.GetStickerMessage(); mm != nil {
		return fieldsFrom(mm), true
	}
	return mediaFields{}, false
}

// processHistorySyncEvent updates messages.media_key for any rows currently
// holding NULL keys when WhatsApp re-delivers them via history sync.
//
// COALESCE in the UPDATE makes this a no-op for rows that already have keys,
// so re-running on overlapping history chunks is safe.
func (b *Bridge) processHistorySyncEvent(evt *events.HistorySync) {
	if evt == nil || evt.Data == nil {
		return
	}
	chunk := evt.Data
	convs := chunk.GetConversations()
	if len(convs) == 0 {
		return
	}

	updated := 0
	scanned := 0
	mediaSeen := 0
	for _, conv := range convs {
		log.Printf("history_sync: conversation %s has %d messages", conv.GetID(), len(conv.GetMessages()))
		for _, hsMsg := range conv.GetMessages() {
			scanned++
			wm := hsMsg.GetMessage()
			if wm == nil {
				continue
			}
			key := wm.GetKey()
			if key == nil || key.GetID() == "" {
				continue
			}
			fields, ok := extractFromMessage(wm.GetMessage())
			if !ok || len(fields.MediaKey) == 0 {
				continue
			}
			mediaSeen++
			caption, msgType := extractContent(unwrapMessage(wm.GetMessage()))
			scrubbed, flags := Scrub(caption)

			// Only fill in fields that are currently NULL. This protects
			// rows where the live receive path already captured them
			// (post-patch messages). It also makes overlapping HistorySync
			// chunks idempotent. Rows stored as "system" because the live
			// path missed a wrapper (e.g. album children) also get their
			// real type + caption, so /api/media/download accepts them.
			res, err := b.db.Exec(`
				UPDATE messages
				   SET content_text         = CASE WHEN type = 'system' AND COALESCE(content_text, '') = '' THEN ? ELSE content_text END,
				       content_normalized   = CASE WHEN type = 'system' AND COALESCE(content_text, '') = '' THEN ? ELSE content_normalized END,
				       scrubbed_text        = CASE WHEN type = 'system' AND COALESCE(content_text, '') = '' THEN ? ELSE scrubbed_text END,
				       scrub_flags_json     = CASE WHEN type = 'system' AND COALESCE(content_text, '') = '' THEN ? ELSE scrub_flags_json END,
				       type                 = CASE WHEN type = 'system' THEN ? ELSE type END,
				       media_key            = COALESCE(media_key, ?),
				       media_direct_path    = COALESCE(media_direct_path, ?),
				       media_url            = COALESCE(media_url, ?),
				       media_enc_sha256     = COALESCE(media_enc_sha256, ?),
				       media_sha256         = COALESCE(media_sha256, ?),
				       media_file_length    = COALESCE(media_file_length, ?),
				       media_key_timestamp  = COALESCE(media_key_timestamp, ?),
				       media_mime           = COALESCE(media_mime, ?)
				 WHERE id = ?
				   AND media_key IS NULL
			`,
				caption, Normalize(caption), scrubbed, ScrubFlagsJSON(flags), msgType,
				fields.MediaKey, fields.MediaDirectPath, fields.MediaURL,
				fields.MediaEncSHA, fields.MediaSHA, fields.MediaFileLength,
				fields.MediaKeyTimestamp, fields.MediaMime, key.GetID(),
			)
			if err != nil {
				log.Printf("history_sync: update %s failed: %v", key.GetID(), err)
				continue
			}
			n, _ := res.RowsAffected()
			updated += int(n)
		}
	}
	log.Printf("history_sync: scanned %d msgs across %d conversations (progress=%d), saw %d media-bearing, backfilled %d rows",
		scanned, len(convs), chunk.GetProgress(), mediaSeen, updated)
}

// RequestChatHistory triggers WhatsApp to deliver `count` messages immediately
// before the oldest message we currently have for `chatJID`. The response
// arrives asynchronously as one or more events.HistorySync events, processed
// by processHistorySyncEvent.
//
// Returns the oldest known message id we anchored on, plus the SendResponse
// from whatsmeow so the operator can correlate.
//
// If no messages exist for the chat yet, returns an error — there's nothing
// to anchor the request on. (For brand-new chats, just wait for live receive.)
func (b *Bridge) RequestChatHistory(ctx context.Context, chatJID string, count int) (anchorID string, resp whatsmeow.SendResponse, err error) {
	if !b.IsConnected() {
		err = errors.New("bridge not connected; cannot request history")
		return
	}
	if count <= 0 {
		count = 50
	}
	if count > 200 {
		count = 200
	}

	// Find the NEWEST message we have for this chat. WhatsApp's
	// HistorySyncOnDemandRequest returns the `count` messages immediately
	// BEFORE the given anchor — so anchoring on the newest gives us the
	// historical chunk we already have rows for (but where media-key
	// fields are NULL because they weren't persisted at receive time).
	// WhatsApp re-delivers the full proto including media keys, and
	// processHistorySyncEvent fills them in via COALESCE.
	//
	// Earlier mistake: anchoring on the OLDEST message asks for pre-history
	// (messages before the chat started), which always returns zero results.
	var (
		msgID    string
		ts       int64
		isFromMe int
	)
	err = b.db.QueryRowContext(ctx, `
		SELECT id, timestamp, is_from_me
		FROM messages
		WHERE chat_jid = ?
		ORDER BY timestamp DESC
		LIMIT 1
	`, chatJID).Scan(&msgID, &ts, &isFromMe)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("no existing messages for chat %s; cannot anchor history request", chatJID)
			return
		}
		err = fmt.Errorf("query oldest message: %w", err)
		return
	}
	anchorID = msgID

	chat, parseErr := types.ParseJID(chatJID)
	if parseErr != nil {
		err = fmt.Errorf("invalid chat_jid: %w", parseErr)
		return
	}

	mi := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: isFromMe == 1,
		},
		ID:        msgID,
		Timestamp: time.Unix(ts, 0),
	}
	reqMsg := b.client.BuildHistorySyncRequest(mi, count)

	// Self-peer send. The HistorySyncOnDemandRequest is a peer-data-operation
	// message addressed to our own device; WhatsApp interprets it server-side
	// and replies via the history-sync stream. Setting Peer=true on the
	// SendRequestExtra is required for whatsmeow to route as a peer message.
	ownJID := b.client.Store.ID
	if ownJID == nil {
		err = errors.New("bridge has no device id; cannot send peer message")
		return
	}
	resp, err = b.client.SendMessage(ctx, ownJID.ToNonAD(), reqMsg, whatsmeow.SendRequestExtra{Peer: true})
	if err != nil {
		err = fmt.Errorf("send history-sync request: %w", err)
		return
	}
	log.Printf("history_sync: requested %d messages for %s before %s (ts=%d)",
		count, chatJID, msgID, ts)
	return
}

