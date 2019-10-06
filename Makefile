all: test

test:
	go test ./...

terraform-create:
	cd infrastructure; terraform apply -input=false
