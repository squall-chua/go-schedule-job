// Package json provides a JSON Codec for goschedule.
package json

import (
	"encoding/json"

	gs "github.com/squallchua/goschedule"
)

// Codec is a goschedule.Codec backed by encoding/json.
type Codec struct{}

// New returns the default JSON codec.
func New() gs.Codec { return Codec{} }

func (Codec) Name() string                    { return "json" }
func (Codec) Encode(v any) ([]byte, error)    { return json.Marshal(v) }
func (Codec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
