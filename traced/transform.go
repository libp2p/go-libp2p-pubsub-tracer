package traced

import (
	"reflect"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-pubsub/pb"
)

func structMap(obj interface{}) map[string]interface{} {
	if obj == nil {
		return map[string]interface{}{}
	}

	var (
		ret = make(map[string]interface{})
		typ = reflect.TypeOf(obj)
		val = reflect.Indirect(reflect.ValueOf(obj))
	)

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json") // json tag.
		fieldValue := val.Field(i)
		if tag == "" || tag == "-" || fieldValue.IsZero() { // skip if no tag, or -, or field is zero.
			continue
		}
		tag = strings.Split(tag, ",")[0]
		if ftyp := typ.Field(i).Type; ftyp.Kind() == reflect.Struct ||
			(ftyp.Kind() == reflect.Ptr && ftyp.Elem().Kind() == reflect.Struct) {
			ret[tag] = structMap(fieldValue.Interface())
		} else {
			ret[tag] = fieldValue.Interface()
		}
	}
	return ret
}

func transformRec(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		if innerMap, ok := v.(map[string]interface{}); ok {
			transformRec(innerMap)
			continue
		}
		switch k {
		case "type":
			// replace numeric enum with string
			m["type"] = v.(*pubsub_pb.TraceEvent_Type).String()
		case "timestamp":
			// patch the timestamp to drop the microsecond and nanosecond portions.
			m["timestamp"] = *(v.(*int64)) / int64(time.Millisecond)
		case "peerID":
			// parse the peer ID.
			if id, err := peer.IDFromBytes(v.([]byte)); err == nil {
				m["peerID"] = id.String()
			}
		}
	}
	return m
}
