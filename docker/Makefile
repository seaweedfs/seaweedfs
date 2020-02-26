all: gen

.PHONY : gen

gen:
	cd ../weed; GOOS=linux go build; mv weed ../docker/
	docker-compose -f local-dev-compose.yml -p seaweedfs up
	rm ./weed
