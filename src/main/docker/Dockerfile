FROM java:8u92-jre-alpine

RUN apk --update add bash; apk --update add curl

# Install KAIROSDB
RUN mkdir /opt; cd /opt; \
  curl -L https://github.com/kairosdb/kairosdb/releases/download/v1.1.3/kairosdb-1.1.3-1.tar.gz | \
  tar zxfp -

EXPOSE 8080

ADD deps /opt/kairosdb/lib/
ADD config /opt/kairosdb/conf/
ADD lib /opt/kairosdb/lib/

CMD ["/opt/kairosdb/bin/kairosdb.sh", "run"]
