APP_NAME:=spork
REPO_NAME:=dimitarvdimitrov/spork

all: test

build:
	go build -o bin/$(APP_NAME) ./...

test: build
	go test ./...

update-drone-kubeconfig:
	doctl kubernetes cluster kubeconfig show spork-cluster | base64 > config.yaml
	drone secret update --name kubeconfig --data "`cat config.yaml`" $(REPO_NAME)
#	rm config.yaml

terraform-create:
	cd infrastructure; terraform apply -input=false

spin-up: terraform-create update-drone-kubeconfig