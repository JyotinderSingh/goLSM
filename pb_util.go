package golsm

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Marshals any proto and panics if it fails.
func mustMarshal[T proto.Message](pb T) []byte {
	bytes, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal proto: %v", err))
	}
	return bytes
}

// Unmarshals any proto and panics if it fails.
func mustUnmarshal[T protoreflect.ProtoMessage](bytes []byte, pb T) {
	err := proto.Unmarshal(bytes, pb)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal proto: %v", err))
	}
}
