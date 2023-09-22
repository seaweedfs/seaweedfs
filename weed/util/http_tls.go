package util

import (
	"github.com/spf13/viper"
)

func HttpScheme(part string) string {
	// todo: could make part-specific lookup (filer/volume/master/client)
	LoadConfiguration("security", false)
	if viper.GetString("https.client.ca") != "" {
		return "https://"
	}

	return "http://"
}

func HttpUseTls(part string) bool {
	// todo: could make part-specific lookup (filer/volume/master/client)
	return viper.GetString("https.client.ca") != ""
}
