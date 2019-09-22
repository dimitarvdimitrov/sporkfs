APP_NAME:=spork
REPO_NAME:=dimitarvdimitrov/spork

all: test

test:
	go test ./...

terraform-create:
	cd infrastructure; terraform apply -input=false
