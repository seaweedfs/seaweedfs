package master_pb

func (v *VolumeLocation) IsEmptyUrl() bool {
	return v.Url == "" || v.Url == ":0"
}
