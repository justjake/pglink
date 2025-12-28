package pgwire

import (
	"reflect"
	"testing"
)

func TestParseOptionsParameter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
		wantErr  bool
	}{
		// Basic cases
		{"single -c option", "-c search_path=schema1", map[string]string{"search_path": "schema1"}, false},
		{"single -- option", "--search_path=schema1", map[string]string{"search_path": "schema1"}, false},
		{"multiple options", "-c search_path=schema1 -c application_name=test", map[string]string{"search_path": "schema1", "application_name": "test"}, false},
		{"mixed formats", "-c search_path=schema1 --timezone=UTC", map[string]string{"search_path": "schema1", "timezone": "UTC"}, false},
		{"empty string", "", map[string]string{}, false},
		{"-c with no space", "-csearch_path=schema1", map[string]string{"search_path": "schema1"}, false},

		// Backslash escaping
		{"escaped space in value", `-c search_path=schema1\ schema2`, map[string]string{"search_path": "schema1 schema2"}, false},
		{"escaped backslash", `-c path=C:\\temp`, map[string]string{"path": `C:\temp`}, false},
		{"multiple escapes", `-c val=a\ b\\c`, map[string]string{"val": `a b\c`}, false},

		// Error cases
		{"no equals", "-c searchpath", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOptionsParameter(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseOptionsParameter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ParseOptionsParameter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestUnescapeOption(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{`escaped\ space`, "escaped space"},
		{`double\\backslash`, `double\backslash`},
		{`mixed\ space\\and`, `mixed space\and`},
		{`trailing\`, `trailing\`}, // Trailing backslash preserved
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := unescapeOption(tt.input)
			if got != tt.expected {
				t.Errorf("unescapeOption(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
