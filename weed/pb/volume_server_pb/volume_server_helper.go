package volume_server_pb

func (m *RemoteFile) BackendName() string {
	return m.BackendType + "." + m.BackendId
}
