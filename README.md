# Centipede
An easy-to-use, declarative ETL processor that can ingest JSON, XML, or HTML, do custom transforms, and load to a wide-variety 
of data sinks. 

## Project Requirements
- Go version `1.22` or above

## How to Run
You can run this via go or do a build and run it via the binary. One important note, is the expectation is 
that the input data is a json array, i.e. `[{"key": "val"}, {"key2": "val2"}...]`. If that's not the expectation 
there is a custom json parser that can handle other varieties, such as ndjson, non-array delimited json, single 
json objects; just pass the `--use-custom-parser` flag.

```sh
$ go run main --output <path_to_file.csv> --fields <comma_delimited_keys> --input <json_array.json>
```

or if you've build the binary via `make build`:

```sh
$ bin/centipede --output <path_to_file.csv> --fields <comma_delimited_keys> --input <filename_or_address>
```

To run the sample from the instructions, run `make run`, which will produce a `solution.csv` as the output.

### Examples
- Run with a json array input file
```sh
$ bin/centipede -i test/testdata/dataset_array.json -o myfile.csv
```

- Run with non-array, comma delimited json
```sh
$ bin/centipede -i test/testdata/dataset_array.json -o myfile.csv -c
```

- Run with dataset validation
```sh
$ bin/centipede -i test/testdata/dataset_array.json -o myfile.csv -d
```

### Usage
```sh
Usage:
  centipede [flags]

Flags:
  -f, --fields strings   fields to extract from the input for the csv (default [modified,publisher.name,publisher.subOrganizationOf.name,contactPoint.fn,keyword])
  -h, --help             help for centipede
  -i, --input string     input file
  -o, --output string    output csv file (default "output.csv")
  -d, --validate         run check that dataset json objects are valid
  -v, --verbose          verbose stdout logging (i.e. debug level)
```

## Development
This project utilizes a Makefile as the main entrypoint to development tasks:
```
The following make targets are available:
------------------------------------------
deps                    Downloads go.mod dependencies
lint                    Lints go files by golangci.lint
lint-fix                Lints and fixes found issues (if linter supports) in go files by golangci.lint
build                   Builds the go binary
run                     Runs the program via go run
test                    Runs unit tests (Alias for test.unit)
test.unit               Runs unit tests
static-analysis         Runs gosec and go-consistent static analyzers
package                 Packages/Builds up the application in docker container
doc                     Hosts the Go docs
help                    Lists all make targets available and description
```

### How to Build the binary to run locally
```sh
$ make build
```
