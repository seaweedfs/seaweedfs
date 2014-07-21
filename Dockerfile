FROM cydev/go
RUN go get code.google.com/p/weed-fs/go/weed
EXPOSE 8080
EXPOSE 9333
VOLUME /data
ENTRYPOINT ["weed"]
