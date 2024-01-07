package examples

import (
	"context"
	"testing"

	"github.com/jinzhu/now"
	"github.com/olivere/elastic/v7"
)

func TestDateHistogram(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewDateHistogramAggregation().Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d")

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.DateHistogram("aggs"); found {
		t.Log("actDay", *agg.Buckets[0].KeyAsString, "doc_count", agg.Buckets[0].DocCount)
	}
}

func TestDateHistogram_SubAggregation(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewDateHistogramAggregation().Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d").
		SubAggregation("dau", elastic.NewCardinalityAggregation().Field("uuid"))
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.DateHistogram("aggs"); found {
		t.Log("actDay", *agg.Buckets[0].KeyAsString, "doc_count", agg.Buckets[0].DocCount)
		if dau, found := agg.Buckets[0].Cardinality("dau"); found {
			t.Log("dau", int64(*dau.Value))
		}
	}
}

func TestDateHistogram_RuntimeMappings(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewDateHistogramAggregation().Field("@timestamp").TimeZone("Asia/Shanghai").CalendarInterval("1d").
		SubAggregation(
			"regDay", elastic.NewDateHistogramAggregation().Field("register_time_date").TimeZone("Asia/Shanghai").CalendarInterval("1d").
				SubAggregation("dau", elastic.NewCardinalityAggregation().Field("uuid")),
		)
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("_exists_:register_time"),
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

	//t.Log(printQueryBody(aggs, query, "runtime_mappings", runtimeMappings))

	buckets, err := cli.Search().Index(businessIndex...).RuntimeMappings(runtimeMappings).Query(query).Aggregation("aggs", aggs).Size(10).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.DateHistogram("aggs"); found {
		t.Log("actDay", *agg.Buckets[0].KeyAsString, "doc_count", agg.Buckets[0].DocCount)
		if regDay, found := agg.Buckets[0].DateHistogram("regDay"); found {
			t.Log("regDay", *regDay.Buckets[0].KeyAsString, "doc_count", regDay.Buckets[0].DocCount)
			if dau, found := regDay.Buckets[0].Cardinality("dau"); found {
				t.Log("dau", int64(*dau.Value))
			}
		}
	}
}
