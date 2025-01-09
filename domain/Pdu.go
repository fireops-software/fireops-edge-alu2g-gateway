package domain

import (
	"bytes"
	"encoding/xml"
	"io"

	appError "github.com/fireops-software/fireops-edge-alu2g-gateway/error"
	"golang.org/x/text/encoding/charmap"
)

// ---------------------------------------------------
// Datatypes
// ---------------------------------------------------
type Pdu struct {
	XMLName   xml.Name  `xml:"pdu"`
	OrderList OrderList `xml:"order-list"`
}

type OrderList struct {
	XMLName xml.Name `xml:"order-list"`
	Order   []Order  `xml:"order"`
	Count   uint     `xml:"count,attr"`
}

type Order struct {
	XMLName xml.Name `xml:"order"`
	Key     string   `xml:"key"`
	Origin  struct {
		XMLName xml.Name `xml:"origin"`
		Name    string   `xml:",chardata"`
		Tid     uint     `xml:"tid,attr"`
	} `xml:"origin"`
	ReceiveTad      string `xml:"receive-tad"`
	OperationId     string `xml:"operation-id"`
	Level           uint   `xml:"level"`
	Name            string `xml:"name"`
	OperationName   string `xml:"operation-name"`
	Caller          string `xml:"caller"`
	Location        string `xml:"location"`
	Info            string `xml:"info"`
	Program         string `xml:"program"`
	Status          string `xml:"status"`
	WatchOutTad     string `xml:"watch-out-tad"`
	FinishedTad     string `xml:"finished-tad"`
	DestinationList struct {
		XMLName     xml.Name `xml:"destination-list"`
		Destination []struct {
			XMLName xml.Name `xml:"destination"`
			Name    string   `xml:",chardata"`
			Id      uint     `xml:"id,attr"`
			Index   uint     `xml:"index,attr"`
		} `xml:"destination"`
		Count uint `xml:"count,attr"`
	} `xml:"destination-list"`
	PagingDestinationList struct {
		XMLName           xml.Name `xml:"paging-destination-list"`
		PagingDestination []struct {
			XMLName xml.Name `xml:"paging-destination"`
			Name    string   `xml:",chardata"`
			Id      uint     `xml:"id,attr"`
			Index   uint     `xml:"index,attr"`
		} `xml:"paging-destination"`
		Count uint `xml:"count,attr"`
	} `xml:"paging-destination-list"`
	Index uint `xml:"index,attr"`
}

// ---------------------------------------------------
// Public functions/methods
// ---------------------------------------------------
func createPduFromBytes(data []byte) (*Pdu, error) {
	encoded, _ := charmap.ISO8859_15.NewDecoder().Bytes(data)
	pdu := &Pdu{}
	d := xml.NewDecoder(bytes.NewReader(encoded))
	d.CharsetReader = identReader
	err := d.Decode(pdu)
	return pdu, err
}

func CreateAlertCollection(data []byte) (*AlertCollection, error) {
	pdu, err := createPduFromBytes(data)
	if err != nil {
		return nil, appError.NewErrInvalidData("failed to parse data to pdu - %v", err)
	}
	ac := &AlertCollection{}
	for _, order := range pdu.OrderList.Order {
		ac.Alerts[order.OperationId] = Alert{
			Origin: Origin{
				Tid:  order.Origin.Tid,
				Name: order.Origin.Name,
			},
			ReceiveTad:    order.ReceiveTad,
			OperationName: order.OperationName,
			Program:       order.Program,
			Level:         order.Level,
			Contact: Contact{
				Name:        order.Name,
				PhoneNumber: order.Caller,
			},
			Location:     order.Location,
			Info:         order.Info,
			Destinations: make(map[uint]string),
		}
		for _, destination := range order.DestinationList.Destination {
			ac.Alerts[order.OperationId].Destinations[destination.Id] = destination.Name
		}
	}
	return ac, nil
}

// ---------------------------------------------------
// Private functions/methods
// ---------------------------------------------------
func identReader(encoding string, input io.Reader) (io.Reader, error) {
	return input, nil
}
