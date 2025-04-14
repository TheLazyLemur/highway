package client

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"time"
)

type Client struct {
	conn         net.Conn
	queueName    string
	consumerName string
	encoder      *json.Encoder
	decoder      *json.Decoder
	uri          string
}

func NewClient(uri string, queueName string, consumerName string) *Client {
	return &Client{
		uri:          uri,
		queueName:    queueName,
		consumerName: consumerName,
	}
}

func (c *Client) ConnectAsConsumer() error {
	conn, err := net.Dial("tcp", c.uri)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	err = encoder.Encode(map[string]any{
		"type": "init",
		"message": map[string]any{
			"role": "consumer",
		},
	})
	if err != nil {
		return err
	}

	c.conn = conn
	c.encoder = encoder
	c.decoder = decoder

	return nil
}

func (c *Client) ConnectAsProducer() error {
	conn, err := net.Dial("tcp", c.uri)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	err = encoder.Encode(map[string]any{
		"type": "init",
		"message": map[string]any{
			"role": "producer",
		},
	})
	if err != nil {
		return err
	}

	c.conn = conn
	c.encoder = encoder
	c.decoder = decoder

	return nil
}

func (c *Client) Push(eventType string, payload any) error {
	pload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	err = c.encoder.Encode(map[string]any{
		"type": "push",
		"message": map[string]any{
			"event_type":      eventType,
			"queue_name":      c.queueName,
			"message_payload": string(pload),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Consume(cb func(id int64, eventType string, pl string) error) {
	go func() {
		for {
			time.Sleep(time.Second)
			err := c.encoder.Encode(map[string]any{
				"type": "consume",
				"message": map[string]any{
					"queue_name":    c.queueName,
					"consumer_name": c.consumerName,
				},
			})
			if err != nil {
				if err == io.EOF {
					slog.Info("Connection closed by server")
				}
				return
			}

			var rawData map[string]any
			err = c.decoder.Decode(&rawData)
			if err != nil {
				if err == io.EOF {
					slog.Info("Connection closed by server")
				}
				return
			}

			eventType := rawData["EventType"].(string)
			data := rawData["MessagePayload"].(string)
			eventID := int64(rawData["Id"].(float64))

			if eventType == "" {
				continue
			}

			err = cb(eventID, eventType, data)
			if err != nil {
				slog.Info("Error in callback:", "error", err.Error())
				continue
			}

			ack := map[string]any{
				"type": "ack",
				"message": map[string]any{
					"message_id":    eventID,
					"queue_name":    c.queueName,
					"consumer_name": c.consumerName,
				},
			}
			c.encoder.Encode(ack)
		}
	}()
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}
