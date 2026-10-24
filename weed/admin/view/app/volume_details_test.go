package app

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

func TestVolumeDetailsAccessControls(t *testing.T) {
	for _, role := range []string{"", "admin", dash.RoleReadOnly} {
		for _, readOnly := range []bool{false, true} {
			data := dash.VolumeDetailsData{Volume: dash.VolumeWithTopology{
				VolumeInformationMessage: &master_pb.VolumeInformationMessage{Id: 7, ReadOnly: readOnly},
				Server:                   "node-a",
			}}
			var rendered bytes.Buffer
			ctx := dash.WithAuthContext(t.Context(), "", role, "")
			require.NoError(t, VolumeDetails(data).Render(ctx, &rendered))
			doc, err := html.Parse(&rendered)
			require.NoError(t, err)
			var modes []string
			var walk func(*html.Node)
			walk = func(node *html.Node) {
				if node.Type == html.ElementNode && node.Data == "button" {
					for _, attr := range node.Attr {
						if attr.Key == "data-read-only" {
							modes = append(modes, attr.Val)
						}
					}
				}
				for child := node.FirstChild; child != nil; child = child.NextSibling {
					walk(child)
				}
			}
			walk(doc)
			if role == dash.RoleReadOnly {
				require.Empty(t, modes)
			} else {
				require.Equal(t, []string{"true", "false"}, modes, "both modes must be available even when the displayed state is stale")
			}
		}
	}
}
