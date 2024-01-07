package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/shopspring/decimal"
)

const (
	adCostQueryString = "_exists_:ad_id AND report_source:gdt AND -(active:0 AND register:0)"

	adCostIndex   = "flower-adcost-*"
	calcmetaIndex = "flower-calcmeta"
)

var (
	start, end int64

	host     = []string{"http://192.168.16.30:9200"}
	user     = "admin"
	password = "admin"

	s = "2023-01-01"
	e = "2023-02-01"

	concurrency = 20
)

func init() {
	startTime, _ := time.ParseInLocation(time.DateOnly, s, time.Local)
	start = startTime.Unix()
	endTime, _ := time.ParseInLocation(time.DateOnly, e, time.Local)
	end = endTime.Unix()
}

func main() {
	ctx := context.Background()

	cli, err := NewClient(host, user, password)
	if err != nil {
		panic(err)
	}

	updates, err := getLatestDataFromAdCost(ctx, cli, start, end)
	if err != nil {
		panic(err)
	}

	log.Printf("total updates: %d, worker num: %d", len(updates), concurrency)

	var (
		updatesChan = make(chan *UpdateByQueryItem)
		wg          sync.WaitGroup
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go processMessages(ctx, i, cli, updatesChan, &wg)
	}

	for i := 0; i < len(updates); i++ {
		updatesChan <- &updates[i]
	}
	close(updatesChan)

	wg.Wait()
	log.Println("finished")
}

func getLatestDataFromAdCost(ctx context.Context, cli *elastic.Client, start, end int64) ([]UpdateByQueryItem, error) {
	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("regDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		elastic.NewCompositeAggregationTermsValuesSource("ad_id").Field("ad_id"),
		elastic.NewCompositeAggregationTermsValuesSource("active").Field("active"),
		elastic.NewCompositeAggregationTermsValuesSource("register").Field("register"),
		elastic.NewCompositeAggregationTermsValuesSource("cost").Field("cost"),
		elastic.NewCompositeAggregationTermsValuesSource("show").Field("show"),
	).
		Size(10000)
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery(adCostQueryString),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(start).Lt(end),
	)

	buckets, err := CompositeAggregation(ctx, cli, []string{adCostIndex}, aggs, query)
	if err != nil {
		return nil, err
	}

	updates := make([]UpdateByQueryItem, len(buckets))
	for i, bucket := range buckets {
		regDay := bucket.Key["regDay"].(float64) / 1000
		adId := bucket.Key["ad_id"].(string)
		active := bucket.Key["active"].(float64)
		activeDec := decimal.NewFromFloat(active)
		register := bucket.Key["register"].(float64)
		registerDec := decimal.NewFromFloat(register)
		show := bucket.Key["show"].(float64)
		costDec := decimal.NewFromFloat(bucket.Key["cost"].(float64)).Round(2)

		var registerActive, costActive, costRegister float64
		if active != 0 {
			registerActiveDec := registerDec.DivRound(activeDec, 4)
			if !registerActiveDec.IsZero() {
				registerActive, _ = registerActiveDec.Float64()
			}

			costActiveDec := costDec.DivRound(activeDec, 4)
			if !costActiveDec.IsZero() {
				costActive, _ = costActiveDec.Float64()
			}
		}
		if register != 0 {
			costRegisterDec := costDec.DivRound(registerDec, 4)
			if !costRegisterDec.IsZero() {
				costRegister, _ = costRegisterDec.Float64()
			}
		}
		params := map[string]interface{}{
			"active":          int64(active),
			"register":        int64(register),
			"show":            int64(show),
			"register_active": registerActive,
			"cost_active":     costActive,
			"cost_register":   costRegister,
		}

		var script string
		for key := range params {
			script += fmt.Sprintf("ctx._source[\"%s\"]=params[\"%s\"];", key, key)
		}
		updates[i] = UpdateByQueryItem{
			Index:  calcmetaIndex,
			Query:  fmt.Sprintf("LogType:advertData AND ad_id:%s AND register_time:%d", adId, int64(regDay)),
			Script: elastic.NewScript(script).Params(params).Lang("painless"),
		}
	}
	return updates, nil
}

func processMessages(ctx context.Context, workerID int, cli *elastic.Client, updatesChan <-chan *UpdateByQueryItem, wg *sync.WaitGroup) {
	defer wg.Done()

	var processed int
	for update := range updatesChan {
		err := UpdateByQuery(ctx, cli, update)
		if err != nil {
			panic(err)
		}
		processed++
	}
	log.Printf("worker %d have processed: %d", workerID, processed)
}

func NewClient(host []string, user, password string) (*elastic.Client, error) {
	// 当 es 服务器监听（publish_address）使用内网服务器 ip，而访问（bound_addresses）使用外网IP时，不要设置 client.transport.sniff 为 true。
	// 不设置 client.transport.sniff 时，默认为 false (关闭客户端去嗅探整个集群的状态)。因为在自动发现时会使用内网 IP 进行通信，
	// 导致无法连接到 es 服务器。因此此时需要直接使用 addTransportAddress 方法把集群中其它机器的 ip 地址加到客户端中。
	return elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host...), elastic.SetBasicAuth(user, password))
}

func CompositeAggregation(ctx context.Context, client *elastic.Client, index []string, aggs *elastic.CompositeAggregation, query *elastic.BoolQuery) ([]*elastic.AggregationBucketCompositeItem, error) {
	var (
		allResultCollected bool
		buckets            []*elastic.AggregationBucketCompositeItem
	)

	for !allResultCollected {
		service, err := client.Search().Index(index...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
		if err != nil {
			return nil, err
		}
		if agg, found := service.Aggregations.Composite("aggs"); found {
			if len(agg.AfterKey) == 0 {
				allResultCollected = true
			} else {
				aggs.AggregateAfter(agg.AfterKey)
			}
			buckets = append(buckets, agg.Buckets...)
		}
	}
	return buckets, nil
}

type UpdateByQueryItem struct {
	Index  string
	Query  string
	Script *elastic.Script
}

func UpdateByQuery(ctx context.Context, client *elastic.Client, update *UpdateByQueryItem) error {
	result, err := client.UpdateByQuery().Index(update.Index).Q(update.Query).Script(update.Script).IgnoreUnavailable(true).Do(ctx)
	if err != nil {
		return err
	}

	if len(result.Failures) > 0 {
		bt, _ := json.Marshal(result.Failures)
		log.Println("failures:", string(bt))
	}
	return nil
}
