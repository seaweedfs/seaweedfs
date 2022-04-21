BINARY = weed

SOURCE_DIR = .

all: install

install:
	cd weed; go install

full_install:
	cd weed; go install -tags "elastic gocdk sqlite hdfs"
