package highway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"time"
)

type handlerFunc func(id int64, eventType string, pl string) error

type Client struct {
	conn            net.Conn
	consumerName    string
	encoder         *json.Encoder
	decoder         *json.Decoder
	uri             string
	routeToHandlers map[string][]handlerFunc
	role            string // Can be "producer", "consumer", or "cache"
}

func NewConsumer(uri string, consumerName string) *Client {
	return &Client{
		uri:          uri,
		consumerName: consumerName,
		role:         "consumer",
	}
}

func NewProducer(uri string) *Client {
	return &Client{
		uri:  uri,
		role: "producer",
	}
}

func NewCache(uri string) *Client {
	return &Client{
		uri:  uri,
		role: "cache", // Using string directly here as Highway types aren't importable in client
	}
}

func (c *Client) Connect(ctx context.Context) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.uri)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	err = encoder.Encode(map[string]any{
		"type": "init",
		"message": map[string]any{
			"role": c.role,
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

func (c *Client) ConnectAsConsumer(ctx context.Context) error {
	c.role = "consumer"
	return c.Connect(ctx)
}

func (c *Client) ConnectAsProducer(ctx context.Context) error {
	c.role = "producer"
	return c.Connect(ctx)
}

func (c *Client) ConnectAsCache(ctx context.Context) error {
	c.role = "cache" // Using string directly as Highway types aren't importable in client
	return c.Connect(ctx)
}

func (c *Client) Push(queueName string, eventType string, payload any) (map[string]any, error) {
	pload, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	err = c.encoder.Encode(map[string]any{
		"type": "push",
		"message": map[string]any{
			"event_type":      eventType,
			"queue_name":      queueName,
			"message_payload": string(pload),
		},
	})
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) RegisterHandler(
	route string,
	handler handlerFunc,
) {
	if c.routeToHandlers == nil {
		c.routeToHandlers = make(map[string][]handlerFunc)
	}

	c.routeToHandlers[route] = append(c.routeToHandlers[route], handler)
}

func (c *Client) Run(queueName string) error {
	for {
		err := c.encoder.Encode(map[string]any{
			"type": "consume",
			"message": map[string]any{
				"queue_name":    queueName,
				"consumer_name": c.consumerName,
			},
		})
		if err != nil {
			if err == io.EOF {
				slog.Info("Connection closed by server")
			}
			return err
		}

		var rawData map[string]any
		err = c.decoder.Decode(&rawData)
		if err != nil {
			if err == io.EOF {
				slog.Info("Connection closed by server")
			}
			return err
		}

		eventType := rawData["EventType"].(string)
		data := rawData["MessagePayload"].(string)
		eventID := int64(rawData["Id"].(float64))

		if eventType == "" {
			continue
		}
		// time.Sleep(time.Millisecond * 200)

		handlers := c.routeToHandlers[eventType]

		// TODO: Think about what to do here
		for _, hnd := range handlers {
			err = hnd(eventID, eventType, data)
			if err != nil {
				slog.Info("Error in callback:", "error", err.Error())
				continue
			}
		}

		ack := map[string]any{
			"type": "ack",
			"message": map[string]any{
				"message_id":    eventID,
				"queue_name":    queueName,
				"consumer_name": c.consumerName,
			},
		}
		c.encoder.Encode(ack)
	}
}

func (c *Client) Consume(queueName string, cb handlerFunc) {
	go func() {
		for {
			time.Sleep(time.Second)
			err := c.encoder.Encode(map[string]any{
				"type": "consume",
				"message": map[string]any{
					"queue_name":    queueName,
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
					"queue_name":    queueName,
					"consumer_name": c.consumerName,
				},
			}
			c.encoder.Encode(ack)
		}
	}()
}

// Cache functions
func (c *Client) CacheSet(key string, value string, expiration time.Duration) (map[string]any, error) {
	expirationMs := int64(expiration / time.Millisecond)
	
	err := c.encoder.Encode(map[string]any{
		"type": "cache_set",
		"message": map[string]any{
			"key":        key,
			"value":      value,
			"expiration": expirationMs,
		},
	})
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) CacheGet(key string) (map[string]any, error) {
	err := c.encoder.Encode(map[string]any{
		"type": "cache_get",
		"message": map[string]any{
			"key": key,
		},
	})
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) CacheDelete(key string) (map[string]any, error) {
	err := c.encoder.Encode(map[string]any{
		"type": "cache_delete",
		"message": map[string]any{
			"key": key,
		},
	})
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) CacheClear() (map[string]any, error) {
	err := c.encoder.Encode(map[string]any{
		"type": "cache_clear",
		"message": map[string]any{},
	})
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}
