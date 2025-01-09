package services

import "github.com/uoul/go-common/async"

type INotificationService[T any] interface {
	Run()
	Close() error
	Subscribe() async.Stream[T]
	Unsubscribe(async.Stream[T])
}
