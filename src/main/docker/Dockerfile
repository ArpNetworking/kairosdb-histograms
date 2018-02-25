FROM java:8u92-jre-alpine

RUN apk --update add bash; apk --update add curl

# Install KAIROSDB
RUN mkdir /opt; cd /opt; \
  curl -L https://github.com/kairosdb/kairosdb/releases/download/v1.2.0/kairosdb-1.2.0-1.tar.gz | \
  tar zxfp -

EXPOSE 8080

ADD deps /opt/kairosdb/lib/
ADD config /opt/kairosdb/conf/
ADD lib /opt/kairosdb/lib/

CMD ["/opt/kairosdb/bin/kairosdb.sh", "run"]
