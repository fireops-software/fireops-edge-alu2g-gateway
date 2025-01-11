package services

import "github.com/uoul/go-common/async"

type INotificationService[T any] interface {
	IService
	Subscribe(chBufferSize uint) async.Stream[T]
	Unsubscribe(async.Stream[T])
}
