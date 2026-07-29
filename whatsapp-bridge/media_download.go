// media_download.go — generalized media-field capture + on-demand download
// for any message type (image, video, document, sticker, audio).
//
// Background: the bridge originally only persisted media_key + related
// download fields for AUDIO messages, because voice-note transcription
// was the only consumer. The receipts pipeline (and any other tool that
// needs image/document content) requires the same treatment for other
// media types.
//
// This file provides:
//
//   1. extractDownloadableFields(evt) — pulls media_key + URL + sha + length
//      + mime out of any media-bearing message uniformly. Used by onMessage
//      so all image/video/document/sticker/audio rows persist the fields
//      needed to re-download later.
//
//   2. Bridge.DownloadMedia(ctx, messageID) — reads the persisted fields
//      from the messages row, reconstitutes the right whatsmeow proto
//      message, calls client.Download, writes bytes to the configured
//      media folder, and updates messages.media_path. Idempotent.
//
//   3. Server.handleDownloadMedia — POST /api/media/download endpoint that
//      lets the receipts pipeline (or any other Python consumer) materialize
//      a single message's bytes on demand without flipping the global
//      WHATSAPP_AUTO_DOWNLOAD_MEDIA flag (which would auto-download every
//      media in every chat — unwanted blast radius for one workflow).

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// mediaFields holds the persisted columns the bridge needs to re-download
// any media-bearing message via whatsmeow.Client.Download.
type mediaFields struct {
	MediaKey          []byte
	MediaURL          sql.NullString
	MediaDirectPath   sql.NullString
	MediaEncSHA       []byte
	MediaSHA          []byte
	MediaFileLength   sql.NullInt64
	MediaKeyTimestamp sql.NullInt64
	MediaMime         sql.NullString
}

// downloadable is the minimum interface every media-bearing protobuf message
// implements. Pulled out only to deduplicate the per-type field copy in
// extractDownloadableFields without losing concrete types.
type downloadable interface {
	GetMediaKey() []byte
	GetFileEncSHA256() []byte
	GetFileSHA256() []byte
	GetURL() string
	GetDirectPath() string
	GetFileLength() uint64
	GetMediaKeyTimestamp() int64
	GetMimetype() string
}

func fieldsFrom(m downloadable) mediaFields {
	var f mediaFields
	f.MediaKey = m.GetMediaKey()
	f.MediaEncSHA = m.GetFileEncSHA256()
	f.MediaSHA = m.GetFileSHA256()
	if u := m.GetURL(); u != "" {
		f.MediaURL = sql.NullString{String: u, Valid: true}
	}
	if dp := m.GetDirectPath(); dp != "" {
		f.MediaDirectPath = sql.NullString{String: dp, Valid: true}
	}
	if fl := m.GetFileLength(); fl > 0 {
		f.MediaFileLength = sql.NullInt64{Int64: int64(fl), Valid: true}
	}
	if mkt := m.GetMediaKeyTimestamp(); mkt > 0 {
		f.MediaKeyTimestamp = sql.NullInt64{Int64: mkt, Valid: true}
	}
	if mt := m.GetMimetype(); mt != "" {
		f.MediaMime = sql.NullString{String: mt, Valid: true}
	}
	return f
}

// extractDownloadableFields pulls the download-relevant fields out of any
// media-bearing message. Returns ok=false for text/system/reaction/etc.
func extractDownloadableFields(evt *events.Message) (mediaFields, bool) {
	if m := evt.Message.GetImageMessage(); m != nil {
		return fieldsFrom(m), true
	}
	if m := evt.Message.GetVideoMessage(); m != nil {
		return fieldsFrom(m), true
	}
	if m := evt.Message.GetDocumentMessage(); m != nil {
		return fieldsFrom(m), true
	}
	if m := evt.Message.GetAudioMessage(); m != nil {
		return fieldsFrom(m), true
	}
	if m := evt.Message.GetStickerMessage(); m != nil {
		return fieldsFrom(m), true
	}
	return mediaFields{}, false
}

// DownloadMedia fetches + decrypts the media bytes for a stored message,
// writes them to the configured media folder, updates messages.media_path,
// and returns the file path + mime + size.
//
// Idempotent: if the message already has a non-NULL media_path that exists
// on disk, returns the existing path without re-downloading.
func (b *Bridge) DownloadMedia(ctx context.Context, messageID string) (path, mimeType string, size int64, err error) {
	var (
		msgType                          string
		mediaPath                        sql.NullString
		mediaKey, mediaEncSha, mediaSha  []byte
		directPath, mediaURL, mediaMime  sql.NullString
		fileLength, mediaKeyTimestamp    sql.NullInt64
	)
	row := b.db.QueryRowContext(ctx, `
		SELECT type, media_path, media_key, media_direct_path, media_url,
		       media_enc_sha256, media_sha256, media_file_length,
		       media_key_timestamp, media_mime
		FROM messages WHERE id = ?
	`, messageID)
	if err = row.Scan(&msgType, &mediaPath,
		&mediaKey, &directPath, &mediaURL,
		&mediaEncSha, &mediaSha, &fileLength,
		&mediaKeyTimestamp, &mediaMime); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", 0, fmt.Errorf("message not found: %s", messageID)
		}
		return "", "", 0, fmt.Errorf("query message: %w", err)
	}

	mt := ""
	if mediaMime.Valid {
		mt = mediaMime.String
	}

	// Idempotent fast-path: already downloaded and file still exists.
	if mediaPath.Valid && mediaPath.String != "" {
		if st, statErr := os.Stat(mediaPath.String); statErr == nil && !st.IsDir() {
			return mediaPath.String, mt, st.Size(), nil
		}
	}

	if len(mediaKey) == 0 {
		return "", "", 0, fmt.Errorf("media key not available for message %s (likely received before media-key persistence patch)", messageID)
	}

	dl, err := buildDownloadable(msgType, mediaKey, mediaEncSha, mediaSha,
		mediaURL, directPath, fileLength, mediaKeyTimestamp, mt)
	if err != nil {
		return "", "", 0, err
	}

	bytes, err := b.client.Download(ctx, dl)
	if err != nil {
		return "", "", 0, fmt.Errorf("whatsmeow download: %w", err)
	}

	if err := os.MkdirAll(b.cfg.MediaPath, 0o700); err != nil {
		return "", "", 0, fmt.Errorf("mkdir media path: %w", err)
	}
	ext := extensionForMime(mt, msgType)
	outPath := filepath.Join(b.cfg.MediaPath, messageID+ext)
	if err := os.WriteFile(outPath, bytes, 0o600); err != nil {
		return "", "", 0, fmt.Errorf("write media file: %w", err)
	}

	if _, perr := b.db.ExecContext(ctx,
		`UPDATE messages SET media_path = ?, media_mime = COALESCE(media_mime, ?) WHERE id = ?`,
		outPath, sql.NullString{String: mt, Valid: mt != ""}, messageID,
	); perr != nil {
		// Bytes are on disk; persistence is best-effort. Log + continue.
		log.Printf("DownloadMedia: persist media_path failed for %s: %v", messageID, perr)
	}

	return outPath, mt, int64(len(bytes)), nil
}

// buildDownloadable reconstitutes the right whatsmeow proto message for the
// given DB-stored fields, suitable for client.Download. Mirrors the audio
// reconstitution in backfill.go.
func buildDownloadable(
	msgType string,
	mediaKey, mediaEncSha, mediaSha []byte,
	mediaURL, directPath sql.NullString,
	fileLength, mediaKeyTimestamp sql.NullInt64,
	mt string,
) (whatsmeow.DownloadableMessage, error) {
	switch msgType {
	case "image":
		m := &waE2E.ImageMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "video":
		m := &waE2E.VideoMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "document":
		m := &waE2E.DocumentMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "audio", "voice":
		m := &waE2E.AudioMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	case "sticker":
		m := &waE2E.StickerMessage{
			MediaKey: mediaKey, FileEncSHA256: mediaEncSha, FileSHA256: mediaSha,
		}
		if mt != "" {
			m.Mimetype = strPtr(mt)
		}
		assignDownloadFields(&m.URL, &m.DirectPath, &m.FileLength, &m.MediaKeyTimestamp,
			mediaURL, directPath, fileLength, mediaKeyTimestamp)
		return m, nil
	default:
		return nil, fmt.Errorf("message type %q is not downloadable", msgType)
	}
}

func assignDownloadFields(
	urlPtr **string, directPathPtr **string,
	fileLengthPtr **uint64, mediaKeyTimestampPtr **int64,
	mediaURL, directPath sql.NullString,
	fileLength, mediaKeyTimestamp sql.NullInt64,
) {
	if mediaURL.Valid {
		s := mediaURL.String
		*urlPtr = &s
	}
	if directPath.Valid {
		s := directPath.String
		*directPathPtr = &s
	}
	if fileLength.Valid {
		fl := uint64(fileLength.Int64)
		*fileLengthPtr = &fl
	}
	if mediaKeyTimestamp.Valid {
		mkt := mediaKeyTimestamp.Int64
		*mediaKeyTimestampPtr = &mkt
	}
}

func strPtr(s string) *string { return &s }

// canonicalExtensions pins the extension for the mime types WhatsApp actually
// sends, so the result does not depend on the host's mime database.
//
// This exists because mime.ExtensionsByType is platform-dependent: on Windows
// it reads HKEY_CLASSES_ROOT\MIME\Database\Content Type\<type> and returns the
// matches sorted alphabetically. For image/jpeg that yields ".jfif" before
// ".jpeg"/".jpg", so downloads landed as .jfif — an extension many tools refuse
// to treat as an image, even though the bytes are ordinary JPEG. On Linux/macOS
// the same call returns ".jpeg", which is why this went unnoticed upstream.
var canonicalExtensions = map[string]string{
	"image/jpeg":      ".jpg",
	"image/png":       ".png",
	"image/gif":       ".gif",
	"image/webp":      ".webp",
	"video/mp4":       ".mp4",
	"video/3gpp":      ".3gp",
	"audio/ogg":       ".ogg",
	"audio/mpeg":      ".mp3",
	"audio/mp4":       ".m4a",
	"audio/amr":       ".amr",
	"application/pdf": ".pdf",
}

// extensionForMime returns a filesystem extension for a given mime type,
// falling back to type-based defaults and finally ".bin".
func extensionForMime(mt, msgType string) string {
	if mt != "" {
		if i := strings.IndexByte(mt, ';'); i >= 0 {
			mt = strings.TrimSpace(mt[:i])
		}
		mt = strings.ToLower(strings.TrimSpace(mt))
		if ext, ok := canonicalExtensions[mt]; ok {
			return ext
		}
		if exts, _ := mime.ExtensionsByType(mt); len(exts) > 0 {
			return exts[0]
		}
	}
	switch msgType {
	case "image":
		return ".jpg"
	case "video":
		return ".mp4"
	case "document":
		return ".bin"
	case "audio", "voice":
		return ".ogg"
	case "sticker":
		return ".webp"
	}
	return ".bin"
}

// downloadMediaResponse is the JSON returned by POST /api/media/download.
type downloadMediaResponse struct {
	MessageID string `json:"message_id"`
	Path      string `json:"path"`
	Mime      string `json:"mime,omitempty"`
	Size      int64  `json:"size"`
	CachedHit bool   `json:"cached_hit,omitempty"`
}

// handleDownloadMedia fetches + decrypts media bytes for a single message
// and persists them under the bridge's media folder. The Python pipeline
// (receipts watcher) calls this after seeing a new image/document in
// list_messages to materialize the file before sending to vision.
func (s *Server) handleDownloadMedia(w http.ResponseWriter, r *http.Request) {
	type req struct {
		MessageID string `json:"message_id"`
	}
	var body req
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<14)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if body.MessageID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "message_id required"})
		return
	}

	var preMediaPath sql.NullString
	_ = s.db.QueryRowContext(r.Context(),
		`SELECT media_path FROM messages WHERE id = ?`,
		body.MessageID,
	).Scan(&preMediaPath)

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	path, mt, size, err := s.bridge.DownloadMedia(ctx, body.MessageID)
	if err != nil {
		errMsg := err.Error()
		switch {
		case strings.Contains(errMsg, "message not found"),
			strings.Contains(errMsg, "media key not available"),
			strings.Contains(errMsg, "is not downloadable"):
			writeJSON(w, http.StatusNotFound, errorResponse{Error: errMsg})
		default:
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "download failed", Details: errMsg})
		}
		return
	}

	cached := preMediaPath.Valid && preMediaPath.String == path && size > 0
	writeJSON(w, http.StatusOK, downloadMediaResponse{
		MessageID: body.MessageID,
		Path:      path,
		Mime:      mt,
		Size:      size,
		CachedHit: cached,
	})
}
