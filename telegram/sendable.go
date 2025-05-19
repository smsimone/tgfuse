package telegram

import "bytes"

type Sendable interface {
	GetBuffer() *bytes.Buffer
	GetName() string
}
