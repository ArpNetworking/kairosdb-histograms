# Copyright 2017 Inscope Metrics, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8u212-jre-alpine

MAINTAINER inscopemetrics

EXPOSE 8080

# Configuration
ENV JAVA_OPTS=""

# Build: KairosDb dependencies
RUN apk --no-cache --update add bash

# Build: KairosDb
RUN apk --no-cache --update add curl; \
  mkdir /opt; \
  cd /opt; \
  curl -L https://github.com/kairosdb/kairosdb/releases/download/v1.2.1/kairosdb-1.2.1-1.tar.gz | tar zxfp - ; \
  apk del curl

# Build: Extensions
ADD deps /opt/kairosdb/lib/
ADD config /opt/kairosdb/conf/
ADD lib /opt/kairosdb/lib/

# Entry point
ENTRYPOINT ["/opt/kairosdb/bin/kairosdb.sh", "run"]
