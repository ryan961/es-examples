package main

import (
	"context"
	"fmt"

	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/now"
	"github.com/olivere/elastic/v7"
	"github.com/samber/lo"
)

var (
	//gte = now.BeginningOfDay().AddDate(0, -1, -1).In(time.Local).Unix()
	//lt  = now.EndOfDay().AddDate(0, 0, -1).In(time.Local).Unix()

	gte = 1700841600 // 2023-11-25
	lt  = 1703433600 // 2023-12-25
)

func main() {
	// 初始化 es
	es, err := NewClient([]string{"http://192.168.16.30:9200"}, "admin", "admin")
	if err != nil {
		panic(err)
	}

	log.Println("gte:", gte, "lt:", lt)

	ctx := context.Background()
	// 1. 获取次留还在的用户
	users := stayUsers(ctx, es)

	chunks := lo.Chunk(users, 1000)
	filters := make([]string, len(chunks))
	for i := range chunks {
		filters[i] = strings.Join(chunks[i], " OR ")
	}
	log.Println("chunks size:", len(chunks))

	//dau := dau(ctx, es, filters)

	// 获取用户每天的看广告次数
	//adCounts(ctx, es, filters, dau)

	// 打标签 m3_gold_sum_daily
	//updateByQuery(ctx, es, filters)

	// 开宝箱数据
	//openBox(ctx, es, filters, dau)

	//daus := dauWithoutDnu(ctx, es)
	//dauAdCounts(ctx, es, daus)

	//dauUsers(ctx, es)

	//diamondData(ctx, es, filters, len(users))

	//itemNumber(ctx, es, filters)
}

// 获取次留还登录的用户 id
func stayUsers(ctx context.Context, es *elastic.Client) []string {
	runtimeMappings := elastic.RuntimeMappings{
		"day_n": map[string]interface{}{
			"type": "long",
			"script": map[string]interface{}{
				"source": `if(doc['actTime'].size() != 0 && doc['registerTime'].size() != 0){ 
					long act = doc['actTime'].value; long reg = doc['registerTime'].value; 
					long act_zoned = act - (act + 0) % 86400; 
					long reg_zoned = reg - (reg + 0) % 86400; 
					emit((act_zoned - reg_zoned) / 86400);  
				}`,
			},
		},
	}

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("LogType:loginLog AND registerTime:[1700841600 TO *} AND day_n:1"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	search := es.Search().Index("m3_cn-business-*").RuntimeMappings(runtimeMappings).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		panic(err)
	}

	users := make([]string, len(buckets))
	for i, bucket := range buckets {
		users[i] = strconv.FormatInt(int64(bucket.Key["customerId"].(float64)), 10)
	}
	return users
}

func dau(ctx context.Context, es *elastic.Client, filters []string) map[int64]int64 {
	data := make(map[int64]int64)
	for i, filter := range filters {
		log.Println("chunk:", i)
		queryString := fmt.Sprintf("LogType:loginLog AND customerId:(%s)", filter)
		aggs := elastic.NewCompositeAggregation().Sources(
			elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		).SubAggregation("count", elastic.NewCardinalityAggregation().Field("customerId")).Size(10000)
		query := elastic.NewBoolQuery().Filter(
			elastic.NewQueryStringQuery(queryString),
			elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
		)

		search := es.Search().Index("m3_cn-business-*").Size(0)
		buckets, err := CompositeAggregation(ctx, search, aggs, query)
		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			actDay := int64(bucket.Key["actDay"].(float64) / 1000)

			if count, ok := bucket.Cardinality("count"); ok {
				data[actDay] += int64(*count.Value)
			}
		}
	}

	return data
}

// 获取用户每天的看广告次数
func adCounts(ctx context.Context, es *elastic.Client, filters []string, dau map[int64]int64) {
	//events := []string{
	//	"evt_ad_sign_in",
	//	"evt_ad_used_props",
	//	"evt_ad_recomm_props",
	//	"evt_ad_revival",
	//	"evt_ad_add_strength",
	//}

	events := []string{
		"evt_ad_sign_in",
		"evt_ad_open_box",
		"evt_ad_openbox_extra",
		"evt_ad_add_strength",
		"evt_ad_add_strength_later",
		"evt_ad_add_strength_extra",
		"evt_daily_tasks_extra",
		"evt_ad_recommon_items",
		"evt_ad_ingame_items",
		"evt_ad_revival",
		"evt_ad_end_double",
		"evt_ad_offlinedynamic_double",
		"evt_ad_luckywheel",
	}
	condition := strings.Join(events, " OR ")

	type user struct {
		CustomerId int64
		Data       map[string]int64
		DocCount   int64
	}

	data := make(map[int64]map[int64]*user)
	for i, filter := range filters {
		log.Println("chunk:", i)
		queryString := fmt.Sprintf("msg.isadshow:1 AND msg.event_name:(%s) AND customerId:(%s)", condition, filter)
		aggs := elastic.NewCompositeAggregation().Sources(
			elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
			elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId").MissingBucket(true),
			elastic.NewCompositeAggregationTermsValuesSource("event_name").Field("msg.event_name").MissingBucket(true),
		).Size(10000)
		query := elastic.NewBoolQuery().Filter(
			elastic.NewQueryStringQuery(queryString),
			elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
		)

		search := es.Search().Index("m3_cn-collect-*").Size(0)
		buckets, err := CompositeAggregation(ctx, search, aggs, query)
		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			actDay := int64(bucket.Key["actDay"].(float64) / 1000)
			customerId := int64(bucket.Key["customerId"].(float64))
			eventName := bucket.Key["event_name"].(string)

			if _, ok := data[actDay]; !ok {
				data[actDay] = make(map[int64]*user)
			}

			if _, ok := data[actDay][customerId]; !ok {
				data[actDay][customerId] = &user{
					CustomerId: customerId,
					Data:       make(map[string]int64),
				}
			}

			if eventName != "" {
				data[actDay][customerId].Data[eventName] += bucket.DocCount
			}
			data[actDay][customerId].DocCount += bucket.DocCount
		}
	}

	type m1 struct {
		Zero            int64
		One             int64
		Two             int64
		Three           int64
		Four            int64
		Five            int64
		GreaterThanFive int64
	}

	type adInfo struct {
		count int64
		total int64
	}

	// 获取广告 0/1/2/3/4/5/>5 以上对应的人数 map[actDay][0/1/2/3/4/5/>5]*m1
	adCounts := make(map[int64]*m1)
	// 获取 0～5/>5 对应广告位的人数 map[actDay][0/5][event]*adInfo
	ads := make(map[int64]map[string]map[string]*adInfo)
	for actDay, v := range data {
		for _, u := range v {
			if _, ok := adCounts[actDay]; !ok {
				adCounts[actDay] = &m1{}
			}
			if u.DocCount == 0 {
				adCounts[actDay].Zero++
			}
			if u.DocCount == 1 {
				adCounts[actDay].One++
			}
			if u.DocCount == 2 {
				adCounts[actDay].Two++
			}
			if u.DocCount == 3 {
				adCounts[actDay].Three++
			}
			if u.DocCount == 4 {
				adCounts[actDay].Four++
			}
			if u.DocCount == 5 {
				adCounts[actDay].Five++
			}
			if u.DocCount > 5 {
				adCounts[actDay].GreaterThanFive++
			}

			if _, ok := ads[actDay]; !ok {
				ads[actDay] = map[string]map[string]*adInfo{
					"0~5": make(map[string]*adInfo),
					">=5": make(map[string]*adInfo),
				}
			}
			key := "0~5"
			if u.DocCount >= 5 {
				key = ">=5"
			}
			for event, c := range u.Data {
				if _, ok := ads[actDay][key][event]; !ok {
					ads[actDay][key][event] = &adInfo{}
				}

				ads[actDay][key][event].count++
				ads[actDay][key][event].total += c
			}
		}
	}

	csv1 := [][]string{{"actDay", "dau", "Zero", "One", "Two", "Three", "Four", "Five", "GreaterThanFive"}}
	for actDay, v := range adCounts {
		csv1 = append(csv1, []string{
			time.Unix(actDay, 0).In(time.Local).Format("2006-01-02"),
			fmt.Sprintf("%d", dau[actDay]),
			fmt.Sprintf("%d", dau[actDay]-v.One-v.Two-v.Three-v.Four-v.Five-v.GreaterThanFive),
			fmt.Sprintf("%d", v.One),
			fmt.Sprintf("%d", v.Two),
			fmt.Sprintf("%d", v.Three),
			fmt.Sprintf("%d", v.Four),
			fmt.Sprintf("%d", v.Five),
			fmt.Sprintf("%d", v.GreaterThanFive),
		})
	}
	err := SaveToCsv("adCounts.csv", csv1)
	if err != nil {
		panic(err)
	}

	csv1 = [][]string{
		{
			"actDay",
			"dau",
			"type",
			"eventName",
			"count",
			"total",
			"avg",
		},
	}
	for actDay, v := range ads {
		for key, ad := range v {
			for event, c := range ad {
				csv1 = append(csv1, []string{
					time.Unix(actDay, 0).In(time.Local).Format("2006-01-02"),
					fmt.Sprintf("%d", dau[actDay]),
					key,
					event,
					fmt.Sprintf("%d", c.count),
					fmt.Sprintf("%d", c.total),
					fmt.Sprintf("%.2f", float64(c.total)/float64(c.count)),
				})
			}
		}
	}
	err = SaveToCsv("ads.csv", csv1)
	if err != nil {
		panic(err)
	}
}

func updateByQuery(ctx context.Context, cli *elastic.Client, filters []string) {
	var (
		concurrency = 10
		updatesChan = make(chan *UpdateByQueryItem)
		wg          sync.WaitGroup
	)

	var updates []UpdateByQueryItem
	for _, filter := range filters {
		params := map[string]any{
			"tmp_tag": 1,
		}

		var script string
		for key := range params {
			script += fmt.Sprintf("ctx._source[\"%s\"]=params[\"%s\"];", key, key)
		}

		updates = append(updates, UpdateByQueryItem{
			//Index:  "m3_cn-collect-*",
			Index: "m3_gold_sum_daily",
			//Query:  fmt.Sprintf(`_exists_:msg.event_name AND customerId:(%s) AND actTime:[1700841600 TO *}`, filter),
			Query:  fmt.Sprintf(`customerId:(%s) AND registerTime.date:[2023-11-24 TO *}`, filter),
			Script: elastic.NewScript(script).Params(params).Lang("painless"),
		})

	}

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

func openBox(ctx context.Context, es *elastic.Client, filters []string, dau map[int64]int64) {
	data := make(map[int64]map[int64]int64)
	for i, filter := range filters {
		log.Println("chunk:", i)
		queryString := fmt.Sprintf("msg.event_name:evt_gold_open_box AND customerId:(%s)", filter)
		aggs := elastic.NewCompositeAggregation().Sources(
			elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
			elastic.NewCompositeAggregationTermsValuesSource("boxNum").Field("msg.boxNum"),
		).Size(10000)
		query := elastic.NewBoolQuery().Filter(
			elastic.NewQueryStringQuery(queryString),
			elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
		)

		search := es.Search().Index("m3_cn-collect-*").Size(0)
		buckets, err := CompositeAggregation(ctx, search, aggs, query)
		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			actDay := int64(bucket.Key["actDay"].(float64) / 1000)
			boxNum := int64(bucket.Key["boxNum"].(float64))

			if _, ok := data[actDay]; !ok {
				data[actDay] = map[int64]int64{
					1: 0,
					2: 0,
					3: 0,
					4: 0,
					5: 0,
				}
			}
			data[actDay][boxNum] += bucket.DocCount
		}
	}

	csv := [][]string{
		{
			"actDay",
			"dau",
			"1档",
			"2档",
			"3档",
			"4档",
			"5档",
		},
	}

	for actDay, v := range data {
		csv = append(csv, []string{
			time.Unix(actDay, 0).In(time.Local).Format("2006-01-02"),
			fmt.Sprintf("%d", dau[actDay]),
			fmt.Sprintf("%d", v[1]),
			fmt.Sprintf("%d", v[2]),
			fmt.Sprintf("%d", v[3]),
			fmt.Sprintf("%d", v[4]),
			fmt.Sprintf("%d", v[5]),
		})
	}
	err := SaveToCsv("box.csv", csv)
	if err != nil {
		panic(err)
	}
}

func dauWithoutDnu(ctx context.Context, es *elastic.Client) map[int64]int64 {
	runtimeMappings := elastic.RuntimeMappings{
		"day_n": map[string]interface{}{
			"type": "long",
			"script": map[string]interface{}{
				"source": `if(doc['actTime'].size() != 0 && doc['registerTime'].size() != 0){ 
					long act = doc['actTime'].value; long reg = doc['registerTime'].value; 
					long act_zoned = act - (act + 0) % 86400; 
					long reg_zoned = reg - (reg + 0) % 86400; 
					emit((act_zoned - reg_zoned) / 86400);  
				}`,
			},
		},
	}

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
	).SubAggregation("count", elastic.NewCardinalityAggregation().Field("customerId")).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("LogType:loginLog AND day_n:[1 TO *}"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	search := es.Search().Index("m3_cn-business-*").RuntimeMappings(runtimeMappings).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		panic(err)
	}

	data := make(map[int64]int64)
	for _, bucket := range buckets {
		actDay := int64(bucket.Key["actDay"].(float64) / 1000)

		if count, ok := bucket.Cardinality("count"); ok {
			data[actDay] += int64(*count.Value)
		}
	}
	return data
}

func dauAdCounts(ctx context.Context, es *elastic.Client, dau map[int64]int64) {
	events := []string{
		"evt_ad_sign_in",
		"evt_ad_open_box",
		"evt_ad_openbox_extra",
		"evt_ad_add_strength",
		"evt_ad_add_strength_later",
		"evt_ad_add_strength_extra",
		"evt_daily_tasks_extra",
		"evt_ad_recommon_items",
		"evt_ad_ingame_items",
		"evt_ad_revival",
		"evt_ad_end_double",
		"evt_ad_offlinedynamic_double",
		"evt_ad_luckywheel",
	}
	condition := strings.Join(events, " OR ")

	type user struct {
		CustomerId int64
		Data       map[string]int64
		DocCount   int64
	}
	data := make(map[int64]map[int64]*user)

	runtimeMappings := elastic.RuntimeMappings{
		"day_n": map[string]interface{}{
			"type": "long",
			"script": map[string]interface{}{
				"source": `if(doc['actTime'].size() != 0 && doc['registerTime'].size() != 0){ 
					long act = doc['actTime'].value; long reg = doc['registerTime'].value; 
					long act_zoned = act - (act + 0) % 86400; 
					long reg_zoned = reg - (reg + 0) % 86400; 
					emit((act_zoned - reg_zoned) / 86400);  
				}`,
			},
		},
	}

	queryString := fmt.Sprintf("msg.isadshow:1 AND msg.event_name:(%s) AND day_n:[1 TO *}", condition)
	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId").MissingBucket(true),
		elastic.NewCompositeAggregationTermsValuesSource("event_name").Field("msg.event_name").MissingBucket(true),
	).Size(10000)
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery(queryString),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	search := es.Search().Index("m3_cn-collect-*").RuntimeMappings(runtimeMappings).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		panic(err)
	}

	for _, bucket := range buckets {
		actDay := int64(bucket.Key["actDay"].(float64) / 1000)
		customerId := int64(bucket.Key["customerId"].(float64))
		eventName := bucket.Key["event_name"].(string)

		if _, ok := data[actDay]; !ok {
			data[actDay] = make(map[int64]*user)
		}

		if _, ok := data[actDay][customerId]; !ok {
			data[actDay][customerId] = &user{
				CustomerId: customerId,
				Data:       make(map[string]int64),
			}
		}

		if eventName != "" {
			data[actDay][customerId].Data[eventName] += bucket.DocCount
		}
		data[actDay][customerId].DocCount += bucket.DocCount
	}

	type m1 struct {
		Zero            int64
		One             int64
		Two             int64
		Three           int64
		Four            int64
		Five            int64
		GreaterThanFive int64
	}

	type adInfo struct {
		count int64
		total int64
	}

	// 获取广告 0/1/2/3/4/5/>5 以上对应的人数 map[actDay][0/1/2/3/4/5/>5]*m1
	adCounts := make(map[int64]*m1)
	// 获取 0～5/>5 对应广告位的人数 map[actDay][0/5][event]*adInfo
	ads := make(map[int64]map[string]map[string]*adInfo)
	for actDay, v := range data {
		for _, u := range v {
			if _, ok := adCounts[actDay]; !ok {
				adCounts[actDay] = &m1{}
			}
			if u.DocCount == 0 {
				adCounts[actDay].Zero++
			}
			if u.DocCount == 1 {
				adCounts[actDay].One++
			}
			if u.DocCount == 2 {
				adCounts[actDay].Two++
			}
			if u.DocCount == 3 {
				adCounts[actDay].Three++
			}
			if u.DocCount == 4 {
				adCounts[actDay].Four++
			}
			if u.DocCount == 5 {
				adCounts[actDay].Five++
			}
			if u.DocCount > 5 {
				adCounts[actDay].GreaterThanFive++
			}

			if _, ok := ads[actDay]; !ok {
				ads[actDay] = map[string]map[string]*adInfo{
					"0~5": make(map[string]*adInfo),
					">=5": make(map[string]*adInfo),
				}
			}
			key := "0~5"
			if u.DocCount >= 5 {
				key = ">=5"
			}
			for event, c := range u.Data {
				if _, ok := ads[actDay][key][event]; !ok {
					ads[actDay][key][event] = &adInfo{}
				}

				ads[actDay][key][event].count++
				ads[actDay][key][event].total += c
			}
		}
	}

	csv1 := [][]string{{"actDay", "dau", "Zero", "One", "Two", "Three", "Four", "Five", "GreaterThanFive"}}
	for actDay, v := range adCounts {
		csv1 = append(csv1, []string{
			time.Unix(actDay, 0).In(time.Local).Format("2006-01-02"),
			fmt.Sprintf("%d", dau[actDay]),
			fmt.Sprintf("%d", dau[actDay]-v.One-v.Two-v.Three-v.Four-v.Five-v.GreaterThanFive),
			fmt.Sprintf("%d", v.One),
			fmt.Sprintf("%d", v.Two),
			fmt.Sprintf("%d", v.Three),
			fmt.Sprintf("%d", v.Four),
			fmt.Sprintf("%d", v.Five),
			fmt.Sprintf("%d", v.GreaterThanFive),
		})
	}
	err = SaveToCsv("adCounts.csv", csv1)
	if err != nil {
		panic(err)
	}

	csv1 = [][]string{
		{
			"actDay",
			"dau",
			"type",
			"eventName",
			"人数",
			"总次数",
			"人均次数",
		},
	}
	for actDay, v := range ads {
		for key, ad := range v {
			for event, c := range ad {
				csv1 = append(csv1, []string{
					time.Unix(actDay, 0).In(time.Local).Format("2006-01-02"),
					fmt.Sprintf("%d", dau[actDay]),
					key,
					event,
					fmt.Sprintf("%d", c.count),
					fmt.Sprintf("%d", c.total),
					fmt.Sprintf("%.2f", float64(c.total)/float64(c.count)),
				})
			}
		}
	}
	err = SaveToCsv("ads.csv", csv1)
	if err != nil {
		panic(err)
	}
}

func dauUsers(ctx context.Context, es *elastic.Client) {
	runtimeMappings := elastic.RuntimeMappings{
		"day_n": map[string]interface{}{
			"type": "long",
			"script": map[string]interface{}{
				"source": `if(doc['actTime'].size() != 0 && doc['registerTime'].size() != 0){ 
					long act = doc['actTime'].value; long reg = doc['registerTime'].value; 
					long act_zoned = act - (act + 0) % 86400; 
					long reg_zoned = reg - (reg + 0) % 86400; 
					emit((act_zoned - reg_zoned) / 86400);  
				}`,
			},
		},
	}
	g := now.BeginningOfDay().AddDate(0, 0, -1).In(time.Local).Unix()
	l := now.EndOfDay().AddDate(0, 0, -1).In(time.Local).Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("LogType:loginLog AND day_n:[1 TO *}"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(g).Lt(l),
	)

	search := es.Search().Index("m3_cn-business-*").RuntimeMappings(runtimeMappings).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		panic(err)
	}

	data := make(map[int64]int64)
	for _, bucket := range buckets {
		data[int64(bucket.Key["customerId"].(float64))] = 1
	}

	events := []string{
		"evt_ad_sign_in",
		"evt_ad_open_box",
		"evt_ad_openbox_extra",
		"evt_ad_add_strength",
		"evt_ad_add_strength_later",
		"evt_ad_add_strength_extra",
		"evt_daily_tasks_extra",
		"evt_ad_recommon_items",
		"evt_ad_ingame_items",
		"evt_ad_revival",
		"evt_ad_end_double",
		"evt_ad_offlinedynamic_double",
		"evt_ad_luckywheel",
	}
	condition := strings.Join(events, " OR ")
	queryString := fmt.Sprintf("msg.isadshow:1 AND msg.event_name:(%s)", condition)

	aggs1 := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).Size(10000)
	query1 := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery(queryString),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(g).Lt(l),
	)
	search1 := es.Search().Index("m3_cn-collect-*").Size(0)
	buckets1, err := CompositeAggregation(ctx, search1, aggs1, query1)
	if err != nil {
		panic(err)
	}
	ad := make(map[int64]int64)
	for _, bucket := range buckets1 {
		ad[int64(bucket.Key["customerId"].(float64))] = 1
	}

	csv1 := [][]string{
		{
			"customerId",
		},
	}

	for c := range data {
		if _, ok := ad[c]; !ok {
			csv1 = append(csv1, []string{
				fmt.Sprintf("%d", c),
			})
		}
	}

	err = SaveToCsv("users.csv", csv1)
	if err != nil {
		panic(err)
	}

}

func diamondData(ctx context.Context, es *elastic.Client, filters []string, count int) {
	get := make(map[int64]int64)
	use := make(map[int64]int64)

	getDetail := make(map[int64]map[int64]int64)
	useDetail := make(map[int64]map[int64]int64)

	for i, filter := range filters {
		log.Println("chunk:", i)
		queryString := fmt.Sprintf("LogType:accountFlow AND -actionType:3 AND customerId:(%s)", filter)

		aggs := elastic.NewCompositeAggregation().Sources(
			elastic.NewCompositeAggregationTermsValuesSource("actionType").Field("actionType"),
			elastic.NewCompositeAggregationTermsValuesSource("flowAction").Field("flowAction"),
		).SubAggregation(
			"get", elastic.NewFilterAggregation().Filter(elastic.NewQueryStringQuery("num:[0 TO *}")).
				//SubAggregation("count", elastic.NewCardinalityAggregation().Field("customerId")).
				SubAggregation("sum", elastic.NewSumAggregation().Field("num")),
		).SubAggregation(
			"use", elastic.NewFilterAggregation().Filter(elastic.NewQueryStringQuery("num:{* TO 0}")).
				//SubAggregation("count", elastic.NewCardinalityAggregation().Field("customerId")).
				SubAggregation("sum", elastic.NewSumAggregation().Field("num")),
		).Size(10000)

		query := elastic.NewBoolQuery().Filter(
			elastic.NewQueryStringQuery(queryString),
			elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
		)

		search := es.Search().Index("m3_cn-business-*").Size(0)
		buckets, err := CompositeAggregation(ctx, search, aggs, query)
		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			actionType := int64(bucket.Key["actionType"].(float64))
			flowAction := int64(bucket.Key["flowAction"].(float64))

			if _, ok := getDetail[actionType]; !ok {
				getDetail[actionType] = make(map[int64]int64)
			}
			if _, ok := useDetail[actionType]; !ok {
				useDetail[actionType] = make(map[int64]int64)
			}

			if getFilter, ok := bucket.Filters("get"); ok {
				if count, ok := getFilter.Sum("sum"); ok {
					get[actionType] += int64(*count.Value)

					getDetail[actionType][flowAction] += int64(*count.Value)
				}
			}

			if useFilter, ok := bucket.Filters("use"); ok {
				if count, ok := useFilter.Sum("sum"); ok {
					use[actionType] += int64(*count.Value)

					useDetail[actionType][flowAction] += int64(*count.Value)
				}
			}
		}
	}

	fmt.Println(get)
	fmt.Println(use)

	lastNumbers := itemNumber(ctx, es, filters)

	csv1 := [][]string{
		{
			"actionType",
			"总人数",
			"总获取",
			"总获取",
			"总消耗",
			"人均结余",
			"flowAction",
			"人均获取",
			"人均消耗",
		},
	}

	for actionType, totalGet := range get {
		for flowAction, sum := range getDetail[actionType] {
			csv1 = append(csv1, []string{
				fmt.Sprintf("%d", actionType),
				fmt.Sprintf("%d", count),
				fmt.Sprintf("%d", totalGet),
				fmt.Sprintf("%d", use[actionType]),
				fmt.Sprintf("%.2f", float64(lo.Sum(lastNumbers[actionType]))/float64(count)),
				fmt.Sprintf("%d", flowAction),
				fmt.Sprintf("%.2f", float64(sum)/float64(count)),
				fmt.Sprintf("%.2f", float64(useDetail[actionType][flowAction])/float64(count)),
			})
		}
	}

	if err := SaveToCsv("items.csv", csv1); err != nil {
		panic(err)
	}
}

func itemNumber(ctx context.Context, es *elastic.Client, filters []string) map[int64][]int64 {
	data := make(map[int64][]int64)

	for i, filter := range filters {
		log.Println("chunk:", i)
		queryString := fmt.Sprintf("LogType:accountFlow AND -actionType:3 AND customerId:(%s)", filter)

		aggs := elastic.NewCompositeAggregation().Sources(
			elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
			elastic.NewCompositeAggregationTermsValuesSource("actionType").Field("actionType"),
		).SubAggregation(
			"lastNumber", elastic.NewTopHitsAggregation().
				SortBy(elastic.NewFieldSort("actTime").Desc()).
				DocvalueField("itemNumber"),
		).Size(10000)

		query := elastic.NewBoolQuery().Filter(
			elastic.NewQueryStringQuery(queryString),
			elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
		)

		search := es.Search().Index("m3_cn-business-*").Size(0)
		buckets, err := CompositeAggregation(ctx, search, aggs, query)
		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			actionType := int64(bucket.Key["actionType"].(float64))

			if top, ok := bucket.TopHits("lastNumber"); ok {
				for _, hit := range top.Hits.Hits {
					itemNumber := int64(hit.Fields["itemNumber"].([]interface{})[0].(float64))

					list := data[actionType]
					list = append(list, itemNumber)
					data[actionType] = list
				}
			}
		}
	}

	return data
}
