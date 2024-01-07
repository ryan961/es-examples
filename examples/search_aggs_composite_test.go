package examples

import (
	"context"
	"testing"

	"github.com/jinzhu/now"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/cast"
)

func CompositeAggregation(ctx context.Context, search *elastic.SearchService, aggs *elastic.CompositeAggregation, query elastic.Query) ([]*elastic.AggregationBucketCompositeItem, error) {
	var buckets []*elastic.AggregationBucketCompositeItem

	for {
		service, err := search.Aggregation("aggs", aggs).Query(query).Do(ctx)
		if err != nil {
			return nil, err
		}
		if agg, found := service.Aggregations.Composite("aggs"); found {
			buckets = append(buckets, agg.Buckets...)
			if len(agg.AfterKey) != 0 {
				aggs.AggregateAfter(agg.AfterKey)
			} else {
				break
			}
		}
	}
	return buckets, nil
}

func TestComposite(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("customerId", int64(buckets[0].Key["customerId"].(float64)))
	t.Log("doc_count", buckets[0].DocCount)
}

func TestComposite_SubAggregation(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).
		SubAggregation("lastLoginIn", elastic.NewMaxAggregation().Field("actTime")).
		SubAggregation("lastLevel", elastic.NewMaxAggregation().Field("topLevel")).
		Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("customerId", int64(buckets[0].Key["customerId"].(float64)))
	t.Log("doc_count", buckets[0].DocCount)

	var lastLoginIn, lastLevel float64
	if last, ok := buckets[0].Max("lastLoginIn"); ok {
		lastLoginIn = *last.Value
	}
	if last, ok := buckets[0].Max("lastLevel"); ok {
		if last.Value != nil {
			lastLevel = *last.Value
		}
	}
	t.Log("lastLoginIn", int64(lastLoginIn))
	t.Log("lastLevel", int64(lastLevel))
}

func TestComposite_DateHistogram(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("actDay", int64(buckets[0].Key["actDay"].(float64)/1000))
	t.Log("customerId", int64(buckets[0].Key["customerId"].(float64)))
}

func TestComposite_DateHistogram_RuntimeMappings(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		elastic.NewCompositeAggregationDateHistogramValuesSource("regDay").Field("register_time_date").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
		elastic.NewCompositeAggregationTermsValuesSource("customerId").Field("customerId"),
		elastic.NewCompositeAggregationTermsValuesSource("register_time").Field("register_time"),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	runtimeMappings := elastic.RuntimeMappings{
		"register_time_date": map[string]interface{}{
			"type": "date",
			"script": map[string]interface{}{
				"source": `if(doc['register_time'].size()!=0) {emit(doc['register_time'].value*1000);} else {emit(0);}`,
			},
		},
	}

	//t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).RuntimeMappings(runtimeMappings).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("actDay", int64(buckets[0].Key["actDay"].(float64)/1000))
	t.Log("regDay", int64(buckets[0].Key["regDay"].(float64)/1000))
	t.Log("customerId", int64(buckets[0].Key["customerId"].(float64)))
}

func TestComposite_DateHistogram_Filters(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
	).SubAggregation(
		"dau", elastic.NewFilterAggregation().Filter(elastic.NewQueryStringQuery("LogType:loginLog")).
			SubAggregation("count", elastic.NewCardinalityAggregation().Field("uuid")),
	).SubAggregation(
		"itemNumbers", elastic.NewFilterAggregation().Filter(elastic.NewQueryStringQuery("LogType:itemsLog")).
			SubAggregation("sum", elastic.NewSumAggregation().Field("number")),
	).SubAggregation(
		"levelCount", elastic.NewFilterAggregation().Filter(elastic.NewQueryStringQuery("LogType:scoreLog")),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("actDay", int64(buckets[0].Key["actDay"].(float64)/1000))
	if dauFilter, ok := buckets[0].Filters("dau"); ok {
		if count, ok := dauFilter.Cardinality("count"); ok {
			t.Log("dau", *count.Value)
		}
	}
	if itemFilter, ok := buckets[0].Filters("itemNumbers"); ok {
		if count, ok := itemFilter.Sum("sum"); ok {
			t.Log("itemsLog_number_sum", *count.Value)
		}
	}
	if levelFilter, ok := buckets[0].Filters("levelCount"); ok {
		t.Log("scoreLog_count", cast.ToInt64(string(levelFilter.Aggregations["doc_count"])))
	}
}

func TestComposite_DateHistogram_Range(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewCompositeAggregation().Sources(
		elastic.NewCompositeAggregationDateHistogramValuesSource("actDay").Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d"),
	).SubAggregation(
		"level", elastic.NewRangeAggregation().Field("topLevel").AddRangeWithKey("0~500", 0, 500).AddUnboundedToWithKey("501~", 501),
	).Size(10000)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("LogType:loginLog"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	t.Log(printQueryBody(aggs, query))

	search := cli.Search().Index(businessIndex...).Size(0)
	buckets, err := CompositeAggregation(ctx, search, aggs, query)
	if err != nil {
		t.Fatal(err)
	}

	//for i := range buckets {
	//	bucket := buckets[i]
	//	// ....
	//}

	t.Log("actDay", int64(buckets[0].Key["actDay"].(float64)/1000))

	if r, ok := buckets[0].Range("level"); ok {
		t.Log("level_"+r.Buckets[0].Key, r.Buckets[0].DocCount)
		t.Log("level_"+r.Buckets[1].Key, r.Buckets[1].DocCount)
	}

}
