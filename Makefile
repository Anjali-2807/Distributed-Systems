CC = go build
BIN = bin

all: $(BIN)/server $(BIN)/client

$(BIN)/server: cmd/server/main.go
	mkdir -p $(BIN)
	$(CC) -o $@ $<

$(BIN)/client: cmd/client/main.go
	mkdir -p $(BIN)
	$(CC) -o $@ $<

test:
	go test -v ./...

clean:
	rm -rf $(BIN)

.PHONY: all test clean
