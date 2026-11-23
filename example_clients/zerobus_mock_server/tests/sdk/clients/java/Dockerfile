# SDK matrix client: Java. zerobus-ingest-sdk 1.8.1 (published; the version in the repo's java/pom.xml),
# with its provided dependencies (Arrow, Avro) and protobuf-java resolved through the Maven mirror.
FROM maven:3-eclipse-temurin-17
ARG MAVEN_MIRROR=https://repo.maven.apache.org/maven2
ENV MAVEN_MIRROR=${MAVEN_MIRROR}
WORKDIR /client
COPY settings.xml /root/.m2/settings.xml
COPY pom.xml /client/pom.xml
RUN mvn -q -B dependency:resolve dependency:copy-dependencies -DoutputDirectory=/client/target/lib || true
COPY src /client/src
RUN mkdir -p /client/target/classes && javac -d /client/target/classes -cp "/client/target/lib/*" /client/src/SdkMatrixClient.java
COPY run.sh /client/run.sh
RUN chmod +x /client/run.sh
ENTRYPOINT ["/client/run.sh"]
