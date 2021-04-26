package msgclient

func (mc *MessagingClient) DeleteChannel(chanName string) error {
	return mc.DeleteTopic("chan", chanName)
}
