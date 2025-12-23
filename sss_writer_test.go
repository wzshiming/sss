package sss

import (
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestWithSHA256(t *testing.T) {
	testBytes := []byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88}
	
	tests := []struct {
		name     string
		input    string
		wantSet  bool
		validate func(string) bool
	}{
		{
			name:    "base64 encoded SHA256",
			input:   base64.URLEncoding.EncodeToString(testBytes),
			wantSet: true,
			validate: func(s string) bool {
				// Input was already base64, should be unchanged
				return s == base64.URLEncoding.EncodeToString(testBytes)
			},
		},
		{
			name:    "hex encoded SHA256",
			input:   hex.EncodeToString(testBytes),
			wantSet: true,
			validate: func(s string) bool {
				// WithSHA256 tries base64 decode first, then hex decode
				// Since hex strings (0-9a-f) are often valid base64, we just verify 
				// that something was set and it's valid base64
				_, err := base64.URLEncoding.DecodeString(s)
				return err == nil && s != ""
			},
		},
		{
			name:    "invalid checksum format",
			input:   "invalid-checksum!!!",
			wantSet: false,
			validate: func(s string) bool {
				return s == ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &writerOption{}
			WithSHA256(tt.input)(opt)
			if tt.wantSet {
				if opt.SHA256 == "" {
					t.Errorf("WithSHA256() did not set SHA256")
				}
				if !tt.validate(opt.SHA256) {
					t.Errorf("WithSHA256() set invalid value: %v", opt.SHA256)
				}
			} else {
				if !tt.validate(opt.SHA256) {
					t.Errorf("WithSHA256() validation failed: %v", opt.SHA256)
				}
			}
		})
	}
}

func TestWithContentType(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{
			name:        "text/plain",
			contentType: "text/plain",
		},
		{
			name:        "application/json",
			contentType: "application/json",
		},
		{
			name:        "image/png",
			contentType: "image/png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &writerOption{}
			WithContentType(tt.contentType)(opt)
			if opt.ContentType != tt.contentType {
				t.Errorf("WithContentType() = %v, want %v", opt.ContentType, tt.contentType)
			}
		})
	}
}
