package domain

type Alert struct {
	Origin        Origin          `json:"origin"`
	ReceiveTad    string          `json:"receiveTad"`
	OperationName string          `json:"operationName"`
	Program       string          `json:"program"`
	Level         uint            `json:"level"`
	Contact       Contact         `json:"contact"`
	Location      string          `json:"location"`
	Info          string          `json:"info"`
	Destinations  map[uint]string `json:"destinations"`
}
