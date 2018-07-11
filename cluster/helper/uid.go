package helper

import (
	crand "crypto/rand"
	"fmt"
	"os"
)

// Generate is used to generate a random UUID
func Generate() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

func GenerateNodeID(bind string) (string, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s_%s", hostName, bind), nil
}
