package entities

// Client represents an SSE connection with subscriptions
type Client struct {
	ID               string
	SubscribedTables []string
	EventChan        chan string
}

// NewClient creates a new client instance
func NewClient(id string, subscribedTables []string) *Client {
	return &Client{
		ID:               id,
		SubscribedTables: subscribedTables,
		EventChan:        make(chan string),
	}
}

// IsSubscribedTo checks if the client is subscribed to a specific table
func (c *Client) IsSubscribedTo(table string) bool {
	for _, t := range c.SubscribedTables {
		if t == table {
			return true
		}
	}
	return false
}
