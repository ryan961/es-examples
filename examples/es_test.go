package examples

import (
	"context"
	"encoding/json"
	"testing"
)

func TestInitEsClient(t *testing.T) {
	cli := initEsClient(t)

	healthResponse, err := cli.CatHealth().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	bt, _ := json.Marshal(healthResponse)
	t.Logf("elasticsearch cluster health: %s", string(bt))
}
