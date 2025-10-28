package sss

import (
	"testing"
)

func TestDirectoryDiff(t *testing.T) {
	tests := []struct {
		name     string
		prev     string
		current  string
		expected []string
	}{
		{
			name:     "nested folder",
			prev:     "/path/to/folder",
			current:  "/path/to/folder/folder/file",
			expected: []string{"/path/to/folder/folder"},
		},
		{
			name:     "sibling folders",
			prev:     "/path/to/folder/folder1",
			current:  "/path/to/folder/folder2/file",
			expected: []string{"/path/to/folder/folder2"},
		},
		{
			name:     "file to file in same folder",
			prev:     "/path/to/folder/folder1/file",
			current:  "/path/to/folder/folder2/file",
			expected: []string{"/path/to/folder/folder2"},
		},
		{
			name:     "deeply nested",
			prev:     "/path/to/folder/folder1/file",
			current:  "/path/to/folder/folder2/folder1/file",
			expected: []string{"/path/to/folder/folder2", "/path/to/folder/folder2/folder1"},
		},
		{
			name:     "from root",
			prev:     "/",
			current:  "/path/to/folder/folder/file",
			expected: []string{"/path", "/path/to", "/path/to/folder", "/path/to/folder/folder"},
		},
		{
			name:     "empty prev",
			prev:     "",
			current:  "/path/to/file",
			expected: []string{},
		},
		{
			name:     "empty current",
			prev:     "/path/to/file",
			current:  "",
			expected: []string{},
		},
		{
			name:     "same path",
			prev:     "/path/to/folder",
			current:  "/path/to/folder/file",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := directoryDiff(tt.prev, tt.current)
			if len(got) != len(tt.expected) {
				t.Errorf("directoryDiff() length = %v, want %v", len(got), len(tt.expected))
				t.Errorf("got: %v, want: %v", got, tt.expected)
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("directoryDiff()[%d] = %v, want %v", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestWithStartAfterHint(t *testing.T) {
	opt := &walkOptions{}
	WithStartAfterHint("test/path")(opt)
	if opt.StartAfterHint != "test/path" {
		t.Errorf("WithStartAfterHint() = %v, want %v", opt.StartAfterHint, "test/path")
	}
}
