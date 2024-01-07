package examples

import (
	"encoding/json"
	"testing"

	"github.com/olivere/elastic/v7"
)

var (
	host     = []string{"http://192.168.16.30:9200"}
	user     string
	password string

	businessIndex = []string{"flower-business-*"}
)

func initEsClient(t *testing.T) *elastic.Client {
	// 当 es 服务器监听（publish_address）使用内网服务器 ip，而访问（bound_addresses）使用外网IP时，不要设置 client.transport.sniff 为 true。
	// 不设置 client.transport.sniff 时，默认为 false (关闭客户端去嗅探整个集群的状态)。因为在自动发现时会使用内网 IP 进行通信，
	// 导致无法连接到 es 服务器。因此此时需要直接使用 addTransportAddress 方法把集群中其它机器的 ip 地址加到客户端中。
	cli, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host...), elastic.SetBasicAuth(user, password))
	if err != nil {
		t.Fatalf("fail to init elastic client: %v", err)
	}
	return cli
}

func printQueryBody(aggs elastic.Aggregation, query elastic.Query) (string, error) {
	aggsSource, err := aggs.Source()
	if err != nil {
		return "", err
	}

	querySource, err := query.Source()
	if err != nil {
		return "", err
	}

	body := map[string]any{
		"aggs":  map[string]any{"aggs": aggsSource},
		"query": querySource,
		"size":  0,
	}
	bt, _ := json.Marshal(body)
	return string(bt), nil
}
