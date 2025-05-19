package database

import "fmt"

type SendKeyErr struct {
	Key string
	Err error
}

func (err SendKeyErr) Error() string { return fmt.Sprintf("%s: %s", err.Key, err.Err.Error()) }

func SendFile(obj Keyed) error {
	params := obj.GetKeyParams()
	for _, item := range params {
		if err := putKey(item.Key, item.GetValue()); err != nil {
			return SendKeyErr{Key: item.Key, Err: err}
		}
		fmt.Println("Sent key to etcd", item.Key)
	}
	return nil
}

func Restore(obj Keyed) error {
	params := obj.GetKeyParams()
	for _, item := range params {
		val, err := getKey(item.Key)
		if err != nil {
			return SendKeyErr{Key: item.Key, Err: err}
		}
		fmt.Println("Retrieved key to etcd", item.Key, val)
		item.SetValue(val)
	}
	return nil
}
