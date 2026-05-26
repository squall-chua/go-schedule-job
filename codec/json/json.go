// Package json provides a JSON Codec for goschedule.
package json

import (
	"encoding/json"

	gs "github.com/squall-chua/go-schedule-job"
)

// Codec is a goschedule.Codec backed by encoding/json.
type Codec struct{}

// New returns the default JSON codec.
func New() gs.Codec { return Codec{} }

// Name returns the codec identifier "json".
func (Codec) Name() string { return "json" }

// Encode marshals v to JSON.
func (Codec) Encode(v any) ([]byte, error) { return json.Marshal(v) }

// Decode unmarshals JSON data into v.
func (Codec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
