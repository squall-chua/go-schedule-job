package goschedule

// Codec encodes and decodes typed payloads. Implementations include
// goschedule/codec/json. Typed dispatch helpers that use Codec (RegisterTyped,
// NowTyped, AtTyped, AfterTyped, EveryTyped, CronTyped) are added once the
// corresponding Scheduler methods exist.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	Name() string
}
