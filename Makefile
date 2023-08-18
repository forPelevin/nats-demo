env:
	docker compose up

down:
	docker compose down

consumer:
	go run cmd/consumer/main.go

publisher:
	go run cmd/publisher/main.go