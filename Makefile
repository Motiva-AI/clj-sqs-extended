.PHONY: test devel

test:
	circleci local execute --job build

devel:
	docker-compose up localstack

