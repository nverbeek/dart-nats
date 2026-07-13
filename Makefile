.PHONY: get analyze format test docker-up docker-down cert clean all

all: get analyze test

get:
	dart pub get

analyze:
	dart analyze

format:
	dart format .

test:
	dart test -j 1

docker-up:
	docker compose up -d

docker-down:
	docker compose down

cert:
	mkdir -p test/config
	openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 \
		-out test/config/server-cert.pem \
		-keyout test/config/server-key.pem \
		-subj "/C=US/ST=State/L=City/O=Organization/OU=OrgUnit/CN=localhost"

clean:
	rm -rf .dart_tool/
