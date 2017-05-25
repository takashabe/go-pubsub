SUBPACKAGES := $(shell go list ./... | grep -v /vendor/)

test:
	go test -v $(SUBPACKAGES)

test_memory:
	GO_MESSAGE_QUEUE_CONFIG="testdata/config/memory.yaml" go test ./ -v; go test ./models -v

test_redis:
	GO_MESSAGE_QUEUE_CONFIG="testdata/config/redis.yaml" go test ./ -v; go test ./models -v

test_mysql:
	GO_MESSAGE_QUEUE_CONFIG="testdata/config/mysql.yaml" go test ./ -v; go test ./models -v

test_debug:
	GO_ROUTER_ENABLE_LOGGING=1 GO_MESSAGE_QUEUE_DEBUG=1 go test ./ -v; go test ./models -v

test_all: test_memory test_redis test_mysql
