# Arrowcvt
## Table of Contents
- [Overview](#overview)
- [Installing](#installing)
- [Usage](#usage)
    - [Available commands](#available-commands)
    - [Converting files](#converting-files)
    - [Run HTTP server](#run-http-server)
- [JSON integration format example](#json-integration-format-example)
### Overview
`Arrowcvt` is a tool for converting from [JSON integration format](https://arrow.apache.org/docs/format/Integration.html) to [Arrow IPC format](https://github.com/apache/arrow/blob/master/docs/source/format/Columnar.rst#serialization-and-interprocess-communication-ipc) back and forth.<br/>
This tool is written based on the [Go arrow library](https://github.com/apache/arrow/tree/master/go/arrow).
### Installing
1. `Golang` should be installed. If not, please follow this instruction: [Install Go](https://golang.org/doc/install)
2. Run `go get` to install `arrowcvt`
```
go get github.com/hieua1/arrowcvt
```
### Usage
`arrowcvt [command]`
 
#### Available Commands

| Command | Usage                           |
|---------|---------------------------------|
| help    | Help about any command          |
| file    | File convert                    |
| server  | Server for serving http requests|

For more details, please run the `help` command, e.g. `arrowcvt file help`

#### Converting files
Convert from JSON to Arrow <br/>
`arrowcvt file jsonarr <arrow_file_name> <json_file_name>`

Convert from Arrow to JSON <br/>
`arrowcvt file arrjson <arrow_file_name> <json_file_name>`

#### Run HTTP server
`arrowcvt server run` <br/>
Will run an HTTP server that helps to convert JSON to Arrow format back and forth. <br/>
Please note that the _default port_ of the server is `8080`

### JSON integration format example

| Product     | Price |
|-------------|-------|
| Apple       | 10    |
| NULL        | 20    |
| Broccoli    | NULL  |
| Cauliflower | 40    |

The above table is represented in JSON integration format as following:
```
{
  "schema": {
    "fields": [
      {
        "name": "Product",
        "type": {
          "name": "utf8"
        },
        "nullable": true,
        "children": []
      },
      {
        "name": "Price",
        "type": {"name": "int", "isSigned": true, "bitWidth":32},
        "nullable": true,
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 4,
      "columns": [
        {
          "name": "Product",
          "count": 4,
          "VALIDITY": [1, 0, 1, 1],
          "DATA": ["Apple", "PineApple", "Broccoli", "Cauliflower"]
        },
        {
          "name": "Price",
          "count": 4,
          "VALIDITY": [1, 1, 0, 1],
          "DATA": [10, 20, 30, 40]
        }
      ]
    }
  ]
}
```
