package traced

import (
	"encoding/json"
	"testing"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"
)

func TestTransformJSON(t *testing.T) {
	// messageID doesn't really belong in a graft RPC, but it's good enough for the test.
	j := `{
		"type":11,
		"peerID":"ACQIARIgpu1Htay3Sy0WTwMbcMdHi7VON0zty7lsnXk+bZEFnCE=",
        "timestamp":1607872988308031877,
        "graft":{
           "peerID":"ACQIARIglw6d0HEz+H9OHdJ5ch01rdTZYwiwpmpo/SuEohH9gzs=",
           "topic":"/fil/blocks/testnetnet",
           "messageID":"d77D3i9K01KcCI/hAPbyoZqdSxoxNh8mEFlOwz1u9bU="
        }
	}`

	var trace pubsub_pb.TraceEvent
	err := json.Unmarshal([]byte(j), &trace)
	require.NoError(t, err)

	m := structMap(trace)
	m = transformRec(m)

	require.Equal(t, "GRAFT", m["type"])
	require.Equal(t, int64(1607872988308), m["timestamp"])
	require.Equal(t, "12D3KooWM3yhTbD4CxKpHatSrp6cmevdur7zY6CdaGXZPmcWxYx8", m["peerID"])
	require.Equal(t, "12D3KooWKz2f7vBK3kigKS3sGX6mM9SkqJARNsS7N75mujriSW3U", m["graft"].(map[string]interface{})["peerID"])
}
