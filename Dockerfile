# install docker
# sudo docker build -t seaweed .
# docker run seaweed

FROM golang

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/chrislusf/weed-fs

# Build the weed command inside the container.
RUN go get github.com/chrislusf/weed-fs/go/weed

EXPOSE 8080
EXPOSE 9333
VOLUME /data
ENTRYPOINT ["/go/bin/weed"]