package examples

import (
	"context"
	"testing"

	"github.com/jinzhu/now"
	"github.com/olivere/elastic/v7"
)

func TestFilters(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewFiltersAggregation().
		FilterWithName("loginLog", elastic.NewQueryStringQuery("LogType:loginLog")).
		FilterWithName("itemsLog", elastic.NewQueryStringQuery("LogType:itemsLog")).
		FilterWithName("scoreLog", elastic.NewQueryStringQuery("LogType:scoreLog"))
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.Filters("aggs"); found {
		t.Log("loginLog", agg.NamedBuckets["loginLog"].DocCount)
		t.Log("itemsLog", agg.NamedBuckets["itemsLog"].DocCount)
		t.Log("scoreLog", agg.NamedBuckets["scoreLog"].DocCount)
	}
}

func TestFilters_SubAggregation(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewFiltersAggregation().
		FilterWithName("loginLog", elastic.NewQueryStringQuery("LogType:loginLog")).
		FilterWithName("itemsLog", elastic.NewQueryStringQuery("LogType:itemsLog")).
		FilterWithName("scoreLog", elastic.NewQueryStringQuery("LogType:scoreLog")).
		SubAggregation("count", elastic.NewCardinalityAggregation().Field("uuid"))
	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.Filters("aggs"); found {
		t.Log("loginLog", agg.NamedBuckets["loginLog"].DocCount)

		if unique, found := agg.NamedBuckets["loginLog"].Cardinality("count"); found {
			t.Log("loginLog unique_count", *unique.Value)
		}

		t.Log("itemsLog", agg.NamedBuckets["itemsLog"].DocCount)
		t.Log("scoreLog", agg.NamedBuckets["scoreLog"].DocCount)
	}
}
