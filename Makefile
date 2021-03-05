
all: pg_back

pg_back: *.go
	go build -ldflags="-s -w" .

test:
	go test -coverprofile=cover.out -v

coverage: test
	go tool cover -func=cover.out

coverage-html:
	go tool cover -html=cover.out

install:
	go install .

clean:
	rm -rf test
	-rm cover.out pg_back

.PHONY: all pg_back test coverage coverage-html
