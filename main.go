package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	for k, v := range reviewRating {
		if v.Rating != 0 {
			fmt.Println(k, v.Rating, v.Reviews)
			break
		}

	}

}
