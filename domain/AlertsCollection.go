package domain

type AlertCollection struct {
	Alerts map[string]Alert `json:"alerts"`
}
