// Command glyphastore-version prints the official Go client version for packaging checks.
package main

import (
	"fmt"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
)

func main() {
	fmt.Print(client.Version)
}
