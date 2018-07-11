package pool

import (
	"reflect"

	hcodec "github.com/hashicorp/go-msgpack/codec"
)

// TODO Figure out if we can remove this. This is our fork that is just way
// behind. I feel like its original purpose was to pin at a stable version but
// now we can accomplish this with vendoring.
var HashiMsgpackHandle = func() *hcodec.MsgpackHandle {
	h := &hcodec.MsgpackHandle{RawToString: true}

	// Sets the default type for decoding a map into a nil interface{}.
	// This is necessary in particular because we store the driver configs as a
	// nil interface{}.
	h.MapType = reflect.TypeOf(map[string]interface{}(nil))
	return h
}()
