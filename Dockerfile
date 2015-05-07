FROM progrium/busybox

WORKDIR /opt/weed

RUN opkg-install curl
RUN echo tlsv1 >> ~/.curlrc

RUN \
  curl -Lks https://bintray.com$(curl -Lk http://bintray.com/chrislusf/seaweedfs/seaweedfs/_latestVersion | grep linux_amd64.tar.gz | sed -n "/href/ s/.*href=['\"]\([^'\"]*\)['\"].*/\1/gp") | gunzip | tar -xf - -C /opt/weed/ && \
  mkdir ./bin &&  mv weed_*/* ./bin && \
  chmod +x ./bin/weed

EXPOSE 8080
EXPOSE 9333

VOLUME /data

ENV WEED_HOME /opt/weed
ENV PATH ${PATH}:${WEED_HOME}/bin

ENTRYPOINT ["weed"]
