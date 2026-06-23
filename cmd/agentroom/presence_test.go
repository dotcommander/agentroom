package main

import (
	"reflect"
	"testing"
)

func TestPresenceLinesShape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		pres   map[string]string
		selfID string
		want   []string
	}{
		{
			name:   "empty",
			pres:   map[string]string{},
			selfID: "",
			want:   []string{"(nobody else here)"},
		},
		{
			name:   "with description",
			pres:   map[string]string{"alice": "builder: refactor auth"},
			selfID: "",
			want:   []string{"  alice -- builder: refactor auth"},
		},
		{
			name:   "empty description renders id only",
			pres:   map[string]string{"bob": ""},
			selfID: "",
			want:   []string{"  bob"},
		},
		{
			name:   "self is omitted",
			pres:   map[string]string{"me": "x", "other": "y"},
			selfID: "me",
			want:   []string{"  other -- y"},
		},
		{
			name:   "self-only collapses to nobody",
			pres:   map[string]string{"me": "x"},
			selfID: "me",
			want:   []string{"(nobody else here)"},
		},
		{
			name:   "sorted by id",
			pres:   map[string]string{"zoe": "", "amy": ""},
			selfID: "",
			want:   []string{"  amy", "  zoe"},
		},
		{
			name:   "json value with claims renders capacity suffix",
			pres:   map[string]string{"alice": `{"desc":"builder: refactor auth","claims":2}`},
			selfID: "",
			want:   []string{"  alice -- builder: refactor auth (2 claimed)"},
		},
		{
			name:   "json value zero claims omits suffix",
			pres:   map[string]string{"alice": `{"desc":"builder: refactor auth","claims":0}`},
			selfID: "",
			want:   []string{"  alice -- builder: refactor auth"},
		},
		{
			name:   "legacy flat string degrades to desc-only",
			pres:   map[string]string{"bob": "role: y"},
			selfID: "",
			want:   []string{"  bob -- role: y"},
		},
		{
			name:   "json value empty desc with claims renders id and suffix",
			pres:   map[string]string{"carol": `{"desc":"","claims":3}`},
			selfID: "",
			want:   []string{"  carol (3 claimed)"},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := presenceLines(tt.pres, tt.selfID)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("presenceLines(%v, %q) = %v, want %v", tt.pres, tt.selfID, got, tt.want)
			}
		})
	}
}
