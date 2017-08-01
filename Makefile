SUBPACKAGES := $(shell go list ./... | grep -v /vendor/)
TEST_MYSQL := GO_MESSAGE_QUEUE_TEST_DATASTORE="mysql"; GO_MESSAGE_QUEUE_TEST_DSN="mq@tcp(localhost:3306)/mq"
TEST_REDIS := GO_MESSAGE_QUEUE_TEST_DATASTORE="redis"
SHOW_ENV := $(shell env | grep GO_MESSAGE_QUEUE)

.PHONY: test_all vet lint

test:
	$(SHOW_ENV)
	go test -v $(SUBPACKAGES)

test_memory:
	$(SHOW_ENV)
	go test -v $(SUBPACKAGES)

test_redis:
	$(SHOW_ENV)
	$(TEST_REDIS) go test -v $(SUBPACKAGES)

test_mysql:
	$(SHOW_ENV)
	$(TEST_MYSQL) go test -v $(SUBPACKAGES)

test_debug:
	GO_ROUTER_ENABLE_LOGGING=1 GO_MESSAGE_QUEUE_DEBUG=1 go test ./ -v; go test ./models -v

test_all: test_memory test_redis test_mysql

vet:
	go vet $(SUBPACKAGES)

lint:
	golint $(SUBPACKAGES)
