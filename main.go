package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v6"
	"io/ioutil"
	"log"
)

type ReviewRating struct {
	Rating  int16    `json:"rating"`
	Reviews []string `json:"reviews"`
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	var reviewRating map[string]ReviewRating

	data, err := ioutil.ReadFile("media/feed_en.json")
	check(err)

	_ = json.Unmarshal(data, &reviewRating)

	esCfg := elasticsearch.Config{
		Addresses: []string{
			"https://search-elastic-b3plo2wuqcral4nlowypjuodra.ap-southeast-1.es.amazonaws.com/",
		},
	}
	es, err := elasticsearch.NewClient(esCfg)
	check(err)

	for hotelId, reviewRatingValue := range reviewRating {
		var queryBuf bytes.Buffer

		query := map[string]interface{}{
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []map[string]interface{}{
						{
							"term": map[string]interface{}{
								"rateHawkId.keyword": hotelId,
							},
						},
					},
				},
			},
		}

		_ = json.NewEncoder(&queryBuf).Encode(query)

		func() {
			res, err := es.Search(
				es.Search.WithContext(context.Background()),
				es.Search.WithIndex("niffler_hotel"),
				es.Search.WithBody(&queryBuf),
				es.Search.WithTrackTotalHits(true),
			)

			if err != nil {
				log.Fatalf("Error getting response: %s", err)
				return
			}

			defer res.Body.Close()

			var r map[string]interface{}

			_ = json.NewDecoder(res.Body).Decode(&r)

			hits := r["hits"].(map[string]interface{})
			data := hits["hits"].([]interface{})

			for _, v := range data {
				_id := v.(map[string]interface{})["_id"].(string)

				docData := map[string]interface{}{
					"doc": map[string]interface{}{
						"rating": reviewRatingValue.Rating,
					},
				}

				_ = json.NewEncoder(&queryBuf).Encode(docData)

				func() {
					resp, err := es.Update(
						"niffler_hotel",
						_id, &queryBuf,
						es.Update.WithDocumentType("doc"),
					)

					if err != nil {
						fmt.Printf("hotelId: %s, error: %s\n", hotelId, err)
						return
					}

					if resp.IsError() {
						_ = json.NewDecoder(resp.Body).Decode(&r)
						d, _ := json.Marshal(r)
						fmt.Println(string(d))
						log.Printf("[%s] Error indexing document ID=%s", resp.Status(), _id)
					}

					defer resp.Body.Close()

					fmt.Printf("%s updated successfully.\n", hotelId)
				}()
			}
		}()
	}

}
