# Java unit tests for the Zerobus mock lib. Build context is the mock_lib directory.
#   docker build --network host -f java/Dockerfile -t zbmock-java:test .
#   docker run --rm --network host zbmock-java:test
FROM maven:3.9.9-eclipse-temurin-17
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2
ENV MAVEN_MIRROR=${MAVEN_MIRROR}
COPY java/settings.xml /root/.m2/settings.xml
WORKDIR /zbmock/java
COPY java/pom.xml ./pom.xml
RUN mvn -B -q dependency:go-offline
COPY java/src ./src
# The tests read ../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
CMD ["mvn", "-B", "test"]
