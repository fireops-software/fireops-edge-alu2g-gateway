package domain

type AlertCollection struct {
	Alerts map[AlertId]Alert `json:"alerts"`
}
