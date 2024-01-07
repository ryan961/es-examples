package main

import (
	"context"
	"log"

	"github.com/olivere/elastic/v7"
)

func NewClient(host []string, user, password string) (*elastic.Client, error) {
	// 当 es 服务器监听（publish_address）使用内网服务器 ip，而访问（bound_addresses）使用外网IP时，不要设置 client.transport.sniff 为 true。
	// 不设置 client.transport.sniff 时，默认为 false (关闭客户端去嗅探整个集群的状态)。因为在自动发现时会使用内网 IP 进行通信，
	// 导致无法连接到 es 服务器。因此此时需要直接使用 addTransportAddress 方法把集群中其它机器的 ip 地址加到客户端中。
	return elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host...), elastic.SetBasicAuth(user, password))
}

func CompositeAggregation(ctx context.Context, search *elastic.SearchService, aggs *elastic.CompositeAggregation, query elastic.Query) ([]*elastic.AggregationBucketCompositeItem, error) {
	var buckets []*elastic.AggregationBucketCompositeItem

	for {
		service, err := search.Aggregation("aggs", aggs).Query(query).Do(ctx)
		if err != nil {
			return nil, err
		}
		if agg, found := service.Aggregations.Composite("aggs"); found {
			buckets = append(buckets, agg.Buckets...)
			log.Println("composite size:", len(buckets))

			if len(agg.AfterKey) != 0 {
				aggs.AggregateAfter(agg.AfterKey)
			} else {
				break
			}

		}
	}
	return buckets, nil
}
