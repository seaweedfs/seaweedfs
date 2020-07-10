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

	projections := []string{"quiz", "fruit"}

	gjson.ForEachLine(data, func(line gjson.Result) bool {
		println(line.Raw)
		println("+++++++++++")
		results := gjson.GetMany(line.Raw, projections...)
		for _, result := range results {
			println(result.Index, result.Type, result.String())
		}
		println("-----------")
		return true
	})

}

func TestJsonQueryRow(t *testing.T) {

	data := `
	{
		"fruit": "Bl\"ue",
		"size": 6,
		"quiz": "green"
	}

`
	selections := []string{"fruit", "size"}

	isFiltered, values := QueryJson(data, selections, Query{
		Field: "quiz",
		Op:    "=",
		Value: "green",
	})

	if !isFiltered {
		t.Errorf("should have been filtered")
	}

	if values == nil {
		t.Errorf("values should have been returned")
	}

	buf := ToJson(nil, selections, values)
	println(string(buf))

}

func TestJsonQueryNumber(t *testing.T) {

	data := `
	{
		"fruit": "Bl\"ue",
		"size": 6,
		"quiz": "green"
	}

`
	selections := []string{"fruit", "quiz"}

	isFiltered, values := QueryJson(data, selections, Query{
		Field: "size",
		Op:    ">=",
		Value: "6",
	})

	if !isFiltered {
		t.Errorf("should have been filtered")
	}

	if values == nil {
		t.Errorf("values should have been returned")
	}

	buf := ToJson(nil, selections, values)
	println(string(buf))

}
