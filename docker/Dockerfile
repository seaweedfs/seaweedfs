FROM progrium/busybox

RUN opkg-install curl
RUN echo tlsv1 >> ~/.curlrc

RUN curl -Lks https://bintray.com$(curl -Lk http://bintray.com/chrislusf/seaweedfs/seaweedfs/_latestVersion | grep linux_amd64.tar.gz | sed -n "/href/ s/.*href=['\"]\([^'\"]*\)['\"].*/\1/gp") | gunzip | tar -xf - && \
  mv go_*amd64/weed /usr/bin/ && \
  rm -r go_*amd64

EXPOSE 8080
EXPOSE 9333

VOLUME /data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
