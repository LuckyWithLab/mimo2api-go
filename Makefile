APP_NAME := mimo2api-go
CMD_PATH := ./cmd/mimo2api
OUT_DIR := ./bin
RELEASE_FLAGS := -trimpath -ldflags="-s -w"

.PHONY: build
build:
	mkdir -p $(OUT_DIR)
	go build -o $(OUT_DIR)/$(APP_NAME) $(CMD_PATH)

.PHONY: build-release
build-release:
	mkdir -p $(OUT_DIR)
	go build $(RELEASE_FLAGS) -o $(OUT_DIR)/$(APP_NAME) $(CMD_PATH)

.PHONY: test
test:
	go test ./...

.PHONY: clean
clean:
	rm -rf $(OUT_DIR)
