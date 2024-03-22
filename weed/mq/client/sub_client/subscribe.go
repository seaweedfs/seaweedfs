package sub_client

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {
	// loop forever
	sub.doKeepConnectedToSubCoordinator()

	return nil
}
