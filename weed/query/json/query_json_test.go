package json

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestGjson(t *testing.T) {
	data := `
	{
		"quiz": {
			"sport": {
				"q1": {
					"question": "Which one is correct team name in NBA?",
					"options": [
						"New York Bulls",
						"Los Angeles Kings",
						"Golden State Warriros",
						"Huston Rocket"
					],
					"answer": "Huston Rocket"
				}
			},
			"maths": {
				"q1": {
					"question": "5 + 7 = ?",
					"options": [
						"10",
						"11",
						"12",
						"13"
					],
					"answer": "12"
				},
				"q2": {
					"question": "12 - 8 = ?",
					"options": [
						"1",
						"2",
						"3",
						"4"
					],
					"answer": "4"
				}
			}
		}
	}

	{
		"fruit": "Apple",
		"size": "Large",
		"quiz": "Red"
	}

`

	projections := []string{"quiz","fruit"}

	gjson.ForEachLine(data, func(line gjson.Result) bool{
		println(line.String())
		println("+++++++++++")
		results := gjson.GetMany(line.Raw, projections...)
		for _, result := range results {
			println(result.Index, result.Type, result.String())
		}
		println("-----------")
		return true
	})



}
