package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/olivere/elastic/v7"
)

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
