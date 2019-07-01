KairosDb Extensions
===================

<a href="https://raw.githubusercontent.com/InscopeMetrics/kairosdb-extensions/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.org/InscopeMetrics/kairosdb-extensions/">
    <img src="https://travis-ci.org/InscopeMetrics/kairosdb-extensions.png?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.inscopemetrics.kairosdb%22%20a%3A%22kairosdb-extensions%22">
    <img src="https://img.shields.io/maven-central/v/io.inscopemetrics.kairosdb/kairosdb-extensions.svg"
         alt="Maven Artifact">
</a>
<a href="https://hub.docker.com/r/inscopemetrics/kairosdb-extensions">
    <img src="https://img.shields.io/docker/pulls/inscopemetrics/kairosdb-extensions.svg" alt="Docker">
</a>

Provides extensions to [KairosDb](https://kairosdb.github.io/) time series data store.

Setup
-----

### Installing

#### Non-Docker

Non-Docker installations require a compatible version of [KairosDb](https://kairosdb.github.io/). Please refer to the
[pom.xml](pom.xml) at the release tag of your version of this package to determine the compatible version of KairosDb. Then refer to
[KairosDb Documentation](https://kairosdb.github.io/docs/build/html/index.html) for how to install KairosDb. Alternatively, use the
Docker installation which bundles this package with a compatible KairosDb version.

##### Source

Clone the repository and build the source. The artifacts from the build are in `target/kairosdb-extensions-${VERSION}-bin.tgz`
where `${VERSION}` is the current build version. To install, copy the artifact directories recursively into an
appropriate target directory on your KairosDb host(s). For example:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw package -Pno-docker
    kairosdb-extensions> scp -r target/kairosdb-extensions-${VERSION}-bin.tgz my-host.example.com:/opt/kairosdb/

##### Tar.gz

Additionally, KairosDb Extensions releases a `tar.gz` package of its build artifacts which may be obtained from Github releases. To install,
download the archive and explode it. Replace `${VERSION}` with the release version of KairosDb Extensions you are installing.
For example, if your KairosDb host(s) have Internet access you can install directly:

    > ssh -c 'curl -L https://github.com/InscopeMetrics/kairosdb-extensions/releases/download/v${VERSION}/kairosdb-extensions-${VERSION}-bin.tgz | tar -xz -C /var/tmp/kairosdb-extensions/' my-host.example.com

Otherwise, you will need to download locally and distribute it before installing. For example:

    > curl -L https://github.com/InscopeMetrics/kairosdb-extensions/releases/download/v${VERSION}/kairosdb-extensions-${VERSION}-bin.tgz -o /var/tmp/kairosdb-extensions.tgz
    > scp /var/tmp/kairosdb-extensions.tgz my-host.example.com:/var/tmp/
    > ssh -c 'tar -xzf /var/tmp/kairosdb-extensions.tgz -C /opt/kairosdb/' my-host.example.com

##### RPM

Alternatively, each release of KairosDb Extensions also creates an RPM which is available on Github releases. To install,
download the RPM and install it. For example, if your KairosDb host(s) have Internet access you can install
directly:

    > ssh -c 'sudo rpm -i https://github.com/InscopeMetrics/kairosdb-extensions/releases/download/v${VERSION}/kairosdb-extensions-${VERSION}-1.noarch.rpm' my-host.example.com

Otherwise, you will need to download the RPM locally and distribute it before installing. For example:

    > curl -L https://github.com/InscopeMetrics/kairosdb-extensions/releases/download/v${VERSION}/kairosdb-extensions-${VERSION}-1.noarch.rpm -o /var/tmp/kairosdb-extensions.rpm
    > scp /var/tmp/kairosdb-extensions.rpm my-host.example.com:/var/tmp/
    > ssh -c 'rpm -i /var/tmp/kairosdb-extensions.rpm' my-host.example.com

Please note that if your organization has its own authorized package repository you will need to work with your system
administrators to install the KairosDb Extensions RPM into your package repository for installation on your KairosDb
host(s).

#### Docker

Furthermore, if you use Docker each release of KairosDb Extensions also publishes a [Docker image](https://hub.docker.com/r/inscopemetrics/kairosdb-extensions/)
that you can either install directly or extend.

If you install the image directly you will likely need to mount either a local directory or data volume with your
organization specific configuration.

If you extend the image you can embed your configuration file directly in your Docker image.

Please refer to [KairosDb Documentation](https://kairosdb.github.io/docs/build/html/index.html) for details.

### Configuration

You must configure KairosDb to use this plugin. Essentially, you must create a properties file like [inscopemetrics.properties](config/inscopemetrics.properties)
in your KairosDb installation. Please refer to [KairosDb Documentation](https://kairosdb.github.io/docs/build/html/index.html) for details.

### Execution

#### Non-Docker

Please refer to [KairosDb Documentation](https://kairosdb.github.io/docs/build/html/index.html) for how to launch KairosDb.

#### Docker

If you installed KairosDb Extensions using a Docker image then execution is very simple. In general:

    docker run -p 8082:8080 <DOCKER ARGS> inscopemetrics/kairosdb-extensions

For example:

    docker run -p 8082:8080 -e 'JAVA_OPTS=-Xms512m' inscopemetrics/kairosdb-extensions

### Building

Prerequisites:
* [Docker](http://www.docker.com/) (for [Mac](https://docs.docker.com/docker-for-mac/))

Building:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw verify

Building without Docker (will disable integration tests):

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw -Pno-docker verify

To control which verification targets (e.g. Checkstyle, Findbugs, Coverage, etc.) are run please refer to the
[parent-pom](https://github.com/InscopeMetrics/parent-pom) for parameters (e.g. `-DskipAllVerification=true`).

To run the server on port 8082 and its dependencies launched via Docker:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw docker:start

To stop the server and its dependencies run; this is recommended in place of `docker kill` as it will also remove the
container and avoids name conflicts on restart:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw docker:stop

To debug on port 9004 with the server on port 8082 and its dependencies launched via Docker:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw -Ddebug=true docker:start

To use the local version as a dependency in your project you must first install it locally:

    kairosdb-extensions> ./jdk-wrapper.sh ./mvnw install

### Testing

* Unit tests (`test/java/**/*Test.java`) may be run or debugged directly from your IDE.
* Integration tests may be run or debugged directly from your IDE provided an instance of KairosDb and its
dependencies are running locally on the default ports.
* To debug KairosDb while executing an integration test against it simply launch KairosDb for debug,
then attach your IDE and finally run/debug the integration test from your IDE.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Inscope Metrics, 2019