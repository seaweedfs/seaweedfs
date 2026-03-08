IMG ?= sw-block-operator:local
CONTROLLER_GEN ?= $(shell which controller-gen 2>/dev/null)

.PHONY: generate manifests build docker-build test

generate:
	$(CONTROLLER_GEN) object paths="./api/..."

manifests:
	$(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=config/crd/bases

build:
	go build -o bin/operator .

docker-build:
	docker build -t $(IMG) .

test:
	go test ./... -v -count=1

fmt:
	go fmt ./...

vet:
	go vet ./...
