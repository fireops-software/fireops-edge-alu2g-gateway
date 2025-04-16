package services

type IPublishService interface {
	IService
	Publish(exchange string, item any) error
}
