package examples

import (
	"context"
	"testing"

	"github.com/jinzhu/now"
	"github.com/olivere/elastic/v7"
)

func TestTerms(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewTermsAggregation().Field("platform").OrderByCountDesc().Size(10)

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.Terms("aggs"); found {
		t.Log(agg.Buckets[0].Key, agg.Buckets[0].DocCount)
	}
}

func TestTerms_SubAggregation(t *testing.T) {
	cli := initEsClient(t)
	ctx := context.Background()

	gte := now.BeginningOfDay().AddDate(0, 0, -7).Unix()
	lt := now.EndOfDay().Unix()

	aggs := elastic.NewTermsAggregation().Field("appVersion").OrderByCountDesc().Size(10).
		SubAggregation("platform", elastic.NewTermsAggregation().Field("platform"))

	query := elastic.NewBoolQuery().Filter(
		elastic.NewQueryStringQuery("*"),
		elastic.NewRangeQuery("@timestamp").Format("epoch_second").Gte(gte).Lt(lt),
	)

	//t.Log(printQueryBody(aggs, query))

	buckets, err := cli.Search().Index(businessIndex...).Query(query).Aggregation("aggs", aggs).Size(0).Do(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if agg, found := buckets.Aggregations.Terms("aggs"); found {
		t.Log("appVersion", agg.Buckets[0].Key, "doc_count", agg.Buckets[0].DocCount)
		if p, found := agg.Buckets[0].Terms("platform"); found {
			for i := range p.Buckets {
				t.Log("platform", p.Buckets[i].Key, "doc_count", p.Buckets[i].DocCount)
			}
		}
	}
}
