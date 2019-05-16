This repo contains sample [Failify](https://failify.io) test cases for Cassandra. To make things easier for illustration
purposes, the build folder from v3.11.4 of cassandra built on Debian Stretch is included in the repo so anyone who wants to
try the test cases is not required to build Cassandra    from scratch.

To run the test cases, first you need to install the following dependencies:
- Java 8
- Docker 1.13+
- Maven 3.x

Also make sure you can run docker commands with the user you are running the tests with. For more information check
[Failify documentation](https://docs.failify.io).

After installing all the dependencies, you can run the test cases using:

```console
$ cd failify
$ docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v $(pwd):/cas -w /cas -it maven:3.6.0-jdk-8 mvn -f failify/pom.xml test
```