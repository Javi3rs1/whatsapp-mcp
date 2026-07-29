package main

import "testing"

// TestExtensionForMimeIsPlatformIndependent pins the extensions for the mime
// types WhatsApp actually sends.
//
// Regression test: on Windows, mime.ExtensionsByType("image/jpeg") reads the
// registry and returns ".jfif" first (alphabetically before ".jpeg"/".jpg"),
// so every downloaded photo landed as .jfif and tools that key off the
// extension refused to treat it as an image. This test fails on Windows
// against the pre-fix code and passes on every platform after it.
func TestExtensionForMimeIsPlatformIndependent(t *testing.T) {
	cases := []struct {
		name    string
		mime    string
		msgType string
		want    string
	}{
		{"jpeg is .jpg, never .jfif", "image/jpeg", "image", ".jpg"},
		{"jpeg with charset param", "image/jpeg; charset=binary", "image", ".jpg"},
		{"jpeg uppercase", "IMAGE/JPEG", "image", ".jpg"},
		{"jpeg with surrounding space", "  image/jpeg  ", "image", ".jpg"},
		{"png", "image/png", "image", ".png"},
		{"webp sticker", "image/webp", "sticker", ".webp"},
		{"mp4 video", "video/mp4", "video", ".mp4"},
		{"ogg voice note", "audio/ogg; codecs=opus", "voice", ".ogg"},
		{"pdf document", "application/pdf", "document", ".pdf"},
		{"empty mime falls back to msgType", "", "image", ".jpg"},
		{"unknown mime falls back to msgType", "application/x-unknown-thing", "video", ".mp4"},
		{"unknown mime and msgType", "application/x-unknown-thing", "", ".bin"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := extensionForMime(c.mime, c.msgType); got != c.want {
				t.Errorf("extensionForMime(%q, %q) = %q, want %q", c.mime, c.msgType, got, c.want)
			}
		})
	}
}
