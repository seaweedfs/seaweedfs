package client

type MessagingClient struct {
	bootstrapBrokers []string
}

func NewMessagingClient(bootstrapBrokers []string) *MessagingClient {
	return &MessagingClient{
		bootstrapBrokers: bootstrapBrokers,
	}
}
