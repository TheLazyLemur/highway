package ops

type MessageEncoder interface {
	Encode(v any) error
}

type MessageDecoder interface {
	Decode(v any) error
}
