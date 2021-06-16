package usecase

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConverter(t *testing.T) {
	tests := []struct {
		name      string
		jsonInput string
		isError   bool
	}{
		{
			name:      "empty",
			jsonInput: ``,
			isError:   true,
		},
		{
			name: "invalid json input",
			jsonInput: `{
				x: 1
			}`,
			isError: true,
		},
		{
			name:      "nulls",
			jsonInput: makeNullWantJSONs(),
			isError:   false,
		},
		{
			name:      "primitives",
			jsonInput: makePrimitiveWantJSONs(),
			isError:   false,
		},
		{
			name:      "structs",
			jsonInput: makeStructsWantJSONs(),
			isError:   false,
		},
		{
			name:      "lists",
			jsonInput: makeListsWantJSONs(),
			isError:   false,
		},
		{
			name:      "strings",
			jsonInput: makeStringsWantJSONs(),
			isError:   false,
		},
		{
			name:      "fixed_size_lists",
			jsonInput: makeFixedSizeListsWantJSONs(),
			isError:   false,
		},
		{
			name:      "fixed_width_types",
			jsonInput: makeFixedWidthTypesWantJSONs(),
			isError:   false,
		},
		{
			name:      "fixed_size_binaries",
			jsonInput: makeFixedSizeBinariesWantJSONs(),
			isError:   false,
		},
		{
			name:      "intervals",
			jsonInput: makeIntervalsWantJSONs(),
			isError:   false,
		},
		{
			name:      "durations",
			jsonInput: makeDurationsWantJSONs(),
			isError:   false,
		},
		{
			name:      "decimal128",
			jsonInput: makeDecimal128sWantJSONs(),
			isError:   false,
		},
		{
			name:      "maps",
			jsonInput: makeMapsWantJSONs(),
			isError:   false,
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			convert := NewDefaultConverterImpl()
			jsonInputReader := strings.NewReader(test.jsonInput)
			arrowBuffer := &bytes.Buffer{}
			err := convert.JSONToArrow(jsonInputReader, arrowBuffer)
			if test.isError {
				require.Error(t, err)
				return
			}
			require.Nil(t, err)
			jsonOutputWriter := &bytes.Buffer{}
			err = convert.ArrowToJSON(arrowBuffer, jsonOutputWriter)
			require.Nil(t, err)
			var json1, json2 interface{}
			err = json.Unmarshal([]byte(test.jsonInput), &json1)
			require.Nil(t, err, "input is not in json format")
			err = json.Unmarshal(jsonOutputWriter.Bytes(), &json2)
			require.Nil(t, err, "output is not in json format")
			require.True(t, reflect.DeepEqual(json1, json2), "unmatched")
		})
	}
}

func makeNullWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "nulls",
        "type": {
          "name": "null"
        },
        "nullable": true,
        "children": []
      }
    ],
    "metadata": [
      {
        "key": "k1",
        "value": "v1"
      },
      {
        "key": "k2",
        "value": "v2"
      },
      {
        "key": "k3",
        "value": "v3"
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "nulls",
          "count": 5
        }
      ]
    }
  ]
}`
}

func makePrimitiveWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "bools",
        "type": {
          "name": "bool"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int8s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 8
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int16s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 16
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int32s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 32
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "int64s",
        "type": {
          "name": "int",
          "isSigned": true,
          "bitWidth": 64
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint8s",
        "type": {
          "name": "int",
          "bitWidth": 8
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint16s",
        "type": {
          "name": "int",
          "bitWidth": 16
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint32s",
        "type": {
          "name": "int",
          "bitWidth": 32
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "uint64s",
        "type": {
          "name": "int",
          "bitWidth": 64
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "float32s",
        "type": {
          "name": "floatingpoint",
          "precision": "SINGLE"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "float64s",
        "type": {
          "name": "floatingpoint",
          "precision": "DOUBLE"
        },
        "nullable": true,
        "children": []
      }
    ],
    "metadata": [
      {
        "key": "k1",
        "value": "v1"
      },
      {
        "key": "k2",
        "value": "v2"
      },
      {
        "key": "k3",
        "value": "v3"
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -1,
            -2,
            -3,
            -4,
            -5
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-1",
            "0",
            "0",
            "-4",
            "-5"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -11,
            -12,
            -13,
            -14,
            -15
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-11",
            "0",
            "0",
            "-14",
            "-15"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "bools",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            true,
            false,
            true,
            false,
            true
          ]
        },
        {
          "name": "int8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -21,
            -22,
            -23,
            -24,
            -25
          ]
        },
        {
          "name": "int64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-21",
            "0",
            "0",
            "-24",
            "-25"
          ]
        },
        {
          "name": "uint8s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "uint64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "float32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "float64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        }
      ]
    }
  ]
}`
}

func makeStructsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "struct_nullable",
        "type": {
          "name": "struct"
        },
        "nullable": true,
        "children": [
          {
            "name": "f1",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": false,
            "children": []
          },
          {
            "name": "f2",
            "type": {
              "name": "utf8"
            },
            "nullable": false,
            "children": []
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 25,
      "columns": [
        {
          "name": "struct_nullable",
          "count": 25,
          "VALIDITY": [
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "f1",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25,
                -31,
                0,
                0,
                -34,
                -35,
                -41,
                0,
                0,
                -44,
                -45
              ]
            },
            {
              "name": "f2",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                "111",
                "",
                "",
                "444",
                "555",
                "1111",
                "",
                "",
                "1444",
                "1555",
                "2111",
                "",
                "",
                "2444",
                "2555",
                "3111",
                "",
                "",
                "3444",
                "3555",
                "4111",
                "",
                "",
                "4444",
                "4555"
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 25,
      "columns": [
        {
          "name": "struct_nullable",
          "count": 25,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            1,
            0,
            0,
            1,
            1
          ],
          "children": [
            {
              "name": "f1",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                1,
                0,
                0,
                4,
                5,
                11,
                0,
                0,
                14,
                15,
                21,
                0,
                0,
                24,
                25,
                31,
                0,
                0,
                34,
                35,
                41,
                0,
                0,
                44,
                45
              ]
            },
            {
              "name": "f2",
              "count": 25,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                "-111",
                "",
                "",
                "-444",
                "-555",
                "-1111",
                "",
                "",
                "-1444",
                "-1555",
                "-2111",
                "",
                "",
                "-2444",
                "-2555",
                "-3111",
                "",
                "",
                "-3444",
                "-3555",
                "-4111",
                "",
                "",
                "-4444",
                "-4555"
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeListsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "list_nullable",
        "type": {
          "name": "list"
        },
        "nullable": true,
        "children": [
          {
            "name": "item",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                1,
                0,
                0,
                4,
                5,
                11,
                0,
                0,
                14,
                15,
                21,
                0,
                0,
                24,
                25
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            0,
            1
          ],
          "OFFSET": [
            0,
            5,
            10,
            15
          ],
          "children": [
            {
              "name": "item",
              "count": 15,
              "VALIDITY": [
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1,
                1,
                0,
                0,
                1,
                1
              ],
              "DATA": [
                -1,
                0,
                0,
                -4,
                -5,
                -11,
                0,
                0,
                -14,
                -15,
                -21,
                0,
                0,
                -24,
                -25
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 0,
      "columns": [
        {
          "name": "list_nullable",
          "count": 0,
          "OFFSET": [
            0
          ],
          "children": [
            {
              "name": "item",
              "count": 0
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedSizeListsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "fixed_size_list_nullable",
        "type": {
          "name": "fixedsizelist",
          "listSize": 3
        },
        "nullable": true,
        "children": [
          {
            "name": "item",
            "type": {
              "name": "int",
              "isSigned": true,
              "bitWidth": 32
            },
            "nullable": true,
            "children": []
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                1,
                0,
                3,
                11,
                0,
                13,
                21,
                0,
                23
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            1,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                -1,
                0,
                -3,
                -11,
                0,
                -13,
                -21,
                0,
                -23
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 3,
      "columns": [
        {
          "name": "fixed_size_list_nullable",
          "count": 3,
          "VALIDITY": [
            1,
            0,
            1
          ],
          "children": [
            {
              "name": "",
              "count": 9,
              "VALIDITY": [
                1,
                0,
                1,
                1,
                0,
                1,
                1,
                0,
                1
              ],
              "DATA": [
                -1,
                0,
                -3,
                -11,
                0,
                -13,
                -21,
                0,
                -23
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeStringsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "strings",
        "type": {
          "name": "utf8"
        },
        "nullable": false,
        "children": []
      },
      {
        "name": "bytes",
        "type": {
          "name": "binary"
        },
        "nullable": false,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1é",
            "2",
            "3",
            "4",
            "5"
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "31C3A9",
            "32",
            "33",
            "34",
            "35"
          ],
          "OFFSET": [
            0,
            3,
            4,
            5,
            6,
            7
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "22",
            "33",
            "44",
            "55"
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "3131",
            "3232",
            "3333",
            "3434",
            "3535"
          ],
          "OFFSET": [
            0,
            2,
            4,
            6,
            8,
            10
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "strings",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "111",
            "222",
            "333",
            "444",
            "555"
          ]
        },
        {
          "name": "bytes",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "313131",
            "323232",
            "333333",
            "343434",
            "353535"
          ],
          "OFFSET": [
            0,
            3,
            6,
            9,
            12,
            15
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedWidthTypesWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "float16s",
        "type": {
          "name": "floatingpoint",
          "precision": "HALF"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time32ms",
        "type": {
          "name": "time",
          "bitWidth": 32,
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time32s",
        "type": {
          "name": "time",
          "bitWidth": 32,
          "unit": "SECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time64ns",
        "type": {
          "name": "time",
          "bitWidth": 64,
          "unit": "NANOSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "time64us",
        "type": {
          "name": "time",
          "bitWidth": 64,
          "unit": "MICROSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_s",
        "type": {
          "name": "timestamp",
          "unit": "SECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_ms",
        "type": {
          "name": "timestamp",
          "unit": "MILLISECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_us",
        "type": {
          "name": "timestamp",
          "unit": "MICROSECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "timestamp_ns",
        "type": {
          "name": "timestamp",
          "unit": "NANOSECOND",
          "timezone": "UTC"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "date32s",
        "type": {
          "name": "date",
          "unit": "DAY"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "date64s",
        "type": {
          "name": "date",
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "0",
            "0",
            "0",
            "3",
            "4"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -2,
            -1,
            0,
            1,
            2
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-2",
            "0",
            "0",
            "1",
            "2"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "10",
            "0",
            "0",
            "13",
            "14"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -12,
            -11,
            10,
            11,
            12
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-12",
            "0",
            "0",
            "11",
            "12"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "float16s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "time32ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "time32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "time64ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        },
        {
          "name": "time64us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        },
        {
          "name": "timestamp_s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "timestamp_ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "20",
            "0",
            "0",
            "23",
            "24"
          ]
        },
        {
          "name": "date32s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            -22,
            -21,
            20,
            21,
            22
          ]
        },
        {
          "name": "date64s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "-22",
            "0",
            "0",
            "21",
            "22"
          ]
        }
      ]
    }
  ]
}`
}

func makeFixedSizeBinariesWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "fixed_size_binary_3",
        "type": {
          "name": "fixedsizebinary",
          "byteWidth": 3
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303031",
            "303032",
            "303033",
            "303034",
            "303035"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303131",
            "303132",
            "303133",
            "303134",
            "303135"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "fixed_size_binary_3",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "303231",
            "303232",
            "303233",
            "303234",
            "303235"
          ]
        }
      ]
    }
  ]
}`
}

func makeIntervalsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "months",
        "type": {
          "name": "interval",
          "unit": "YEAR_MONTH"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "days",
        "type": {
          "name": "interval",
          "unit": "DAY_TIME"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "months",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            1,
            2,
            3,
            4,
            5
          ]
        },
        {
          "name": "days",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "days": 1,
              "milliseconds": 1
            },
            {
              "days": 2,
              "milliseconds": 2
            },
            {
              "days": 3,
              "milliseconds": 3
            },
            {
              "days": 4,
              "milliseconds": 4
            },
            {
              "days": 5,
              "milliseconds": 5
            }
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "months",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            11,
            12,
            13,
            14,
            15
          ]
        },
        {
          "name": "days",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "days": 11,
              "milliseconds": 11
            },
            {
              "days": 12,
              "milliseconds": 12
            },
            {
              "days": 13,
              "milliseconds": 13
            },
            {
              "days": 14,
              "milliseconds": 14
            },
            {
              "days": 15,
              "milliseconds": 15
            }
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "months",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            21,
            22,
            23,
            24,
            25
          ]
        },
        {
          "name": "days",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            {
              "days": 21,
              "milliseconds": 21
            },
            {
              "days": 22,
              "milliseconds": 22
            },
            {
              "days": 23,
              "milliseconds": 23
            },
            {
              "days": 24,
              "milliseconds": 24
            },
            {
              "days": 25,
              "milliseconds": 25
            }
          ]
        }
      ]
    }
  ]
}`
}

func makeDurationsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "durations-s",
        "type": {
          "name": "duration",
          "unit": "SECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-ms",
        "type": {
          "name": "duration",
          "unit": "MILLISECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-us",
        "type": {
          "name": "duration",
          "unit": "MICROSECOND"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "durations-ns",
        "type": {
          "name": "duration",
          "unit": "NANOSECOND"
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "1",
            "0",
            "0",
            "4",
            "5"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "11",
            "0",
            "0",
            "14",
            "15"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "durations-s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-ms",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-us",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        },
        {
          "name": "durations-ns",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "21",
            "0",
            "0",
            "24",
            "25"
          ]
        }
      ]
    }
  ]
}`
}

func makeDecimal128sWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "dec128s",
        "type": {
          "name": "decimal",
          "scale": 1,
          "precision": 10
        },
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "571849066284996100127",
            "590295810358705651744",
            "608742554432415203361",
            "627189298506124754978",
            "645636042579834306595"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "756316507022091616297",
            "774763251095801167914",
            "793209995169510719531",
            "811656739243220271148",
            "830103483316929822765"
          ]
        }
      ]
    },
    {
      "count": 5,
      "columns": [
        {
          "name": "dec128s",
          "count": 5,
          "VALIDITY": [
            1,
            0,
            0,
            1,
            1
          ],
          "DATA": [
            "940783947759187132467",
            "959230691832896684084",
            "977677435906606235701",
            "996124179980315787318",
            "1014570924054025338935"
          ]
        }
      ]
    }
  ]
}`
}

func makeMapsWantJSONs() string {
	return `{
  "schema": {
    "fields": [
      {
        "name": "map_int_utf8",
        "type": {
          "name": "map",
          "keysSorted": true
        },
        "nullable": true,
        "children": [
          {
            "name": "entries",
            "type": {
              "name": "struct"
            },
            "nullable": false,
            "children": [
              {
                "name": "key",
                "type": {
                  "name": "int",
                  "isSigned": true,
                  "bitWidth": 32
                },
                "nullable": false,
                "children": []
              },
              {
                "name": "value",
                "type": {
                  "name": "utf8"
                },
                "nullable": true,
                "children": []
              }
            ]
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 2,
      "columns": [
        {
          "name": "map_int_utf8",
          "count": 2,
          "VALIDITY": [
            1,
            0
          ],
          "OFFSET": [
            0,
            25,
            50
          ],
          "children": [
            {
              "name": "entries",
              "count": 50,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "children": [
                {
                  "name": "key",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1
                  ],
                  "DATA": [
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5
                  ]
                },
                {
                  "name": "value",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1
                  ],
                  "DATA": [
                    "111",
                    "",
                    "",
                    "444",
                    "555",
                    "1111",
                    "",
                    "",
                    "1444",
                    "1555",
                    "2111",
                    "",
                    "",
                    "2444",
                    "2555",
                    "3111",
                    "",
                    "",
                    "3444",
                    "3555",
                    "4111",
                    "",
                    "",
                    "4444",
                    "4555",
                    "-111",
                    "",
                    "",
                    "-444",
                    "-555",
                    "-1111",
                    "",
                    "",
                    "-1444",
                    "-1555",
                    "-2111",
                    "",
                    "",
                    "-2444",
                    "-2555",
                    "-3111",
                    "",
                    "",
                    "-3444",
                    "-3555",
                    "-4111",
                    "",
                    "",
                    "-4444",
                    "-4555"
                  ]
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "count": 2,
      "columns": [
        {
          "name": "map_int_utf8",
          "count": 2,
          "VALIDITY": [
            1,
            0
          ],
          "OFFSET": [
            0,
            25,
            50
          ],
          "children": [
            {
              "name": "entries",
              "count": 50,
              "VALIDITY": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1
              ],
              "children": [
                {
                  "name": "key",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1
                  ],
                  "DATA": [
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    1,
                    2,
                    3,
                    4,
                    5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5
                  ]
                },
                {
                  "name": "value",
                  "count": 50,
                  "VALIDITY": [
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1,
                    1,
                    0,
                    0,
                    1,
                    1
                  ],
                  "DATA": [
                    "-111",
                    "",
                    "",
                    "-444",
                    "-555",
                    "-1111",
                    "",
                    "",
                    "-1444",
                    "-1555",
                    "-2111",
                    "",
                    "",
                    "-2444",
                    "-2555",
                    "-3111",
                    "",
                    "",
                    "-3444",
                    "-3555",
                    "-4111",
                    "",
                    "",
                    "-4444",
                    "-4555",
                    "111",
                    "",
                    "",
                    "444",
                    "555",
                    "1111",
                    "",
                    "",
                    "1444",
                    "1555",
                    "2111",
                    "",
                    "",
                    "2444",
                    "2555",
                    "3111",
                    "",
                    "",
                    "3444",
                    "3555",
                    "4111",
                    "",
                    "",
                    "4444",
                    "4555"
                  ]
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}`
}
