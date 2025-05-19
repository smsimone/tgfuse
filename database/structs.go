package database

type KeyParam struct {
	Key      string
	GetValue func() string
	SetValue func(string)
}

type Keyed interface {
	GetKeyParams() []KeyParam
}
