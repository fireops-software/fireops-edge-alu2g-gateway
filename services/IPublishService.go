package services

type IPublishService[T any] interface {
	IService
	Publish(item *T) error
}
