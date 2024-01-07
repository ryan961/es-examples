package examples

import (
	"context"
	"testing"

	"github.com/olivere/elastic/v7"
)

func TestDeleteByQuery(t *testing.T) {
	client := initEsClient(t)

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Elasticsearch."}
	tweet2 := tweet{User: "olivere", Message: "Another unrelated topic."}
	tweet3 := tweet{User: "sandrae", Message: "Cycling is fun."}

	// Add all documents
	_, err := client.Index().Index(testIndexName).Id("1").BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("2").BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("3").BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Refresh().Index(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Count documents
	count, err := client.Count(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected count = %d; got: %d", 3, count)
	}

	// Delete all documents by sandrae
	q := elastic.NewTermQuery("user", "sandrae")
	res, err := client.DeleteByQuery().
		Index(testIndexName).
		Query(q).
		Pretty(true).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatalf("expected response != nil; got: %v", res)
	}

	// Refresh and check count
	_, err = client.Refresh().Index(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	count, err = client.Count(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("expected Count = %d; got: %d", 2, count)
	}
}

func TestDeleteByQueryAsync(t *testing.T) {
	client := initEsClient(t)

	tweet1 := tweet{User: "olivere", Message: "Welcome to Golang and Elasticsearch."}
	tweet2 := tweet{User: "olivere", Message: "Another unrelated topic."}
	tweet3 := tweet{User: "sandrae", Message: "Cycling is fun."}

	// Add all documents
	_, err := client.Index().Index(testIndexName).Id("1").BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("2").BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(testIndexName).Id("3").BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Refresh().Index(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Count documents
	count, err := client.Count(testIndexName).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("expected count = %d; got: %d", 3, count)
	}

	// Delete all documents by sandrae
	q := elastic.NewTermQuery("user", "sandrae")
	res, err := client.DeleteByQuery().
		Index(testIndexName).
		Query(q).
		Slices("auto").
		Pretty(true).
		DoAsync(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected result != nil")
	}
	if res.TaskId == "" {
		t.Errorf("expected a task id, got %+v", res)
	}

	tasksGetTask := client.TasksGetTask()
	taskStatus, err := tasksGetTask.TaskId(res.TaskId).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if taskStatus == nil {
		t.Fatal("expected task status result != nil")
	}
}
