FROM maven:3.8.5-jdk-11 AS build
COPY ./src ./src
COPY ./pom.xml ./pom.xml
#RUN mvn -DFRONTIER_QUEUE_NAME=url-frontier -DINDEX_QUEUE_NAME=index-queue -DDOCUMENT_BUCKET_NAME=555documents -DREDIS_HOST=localhost clean package
RUN mvn clean package

FROM gcr.io/distroless/java
#COPY --from=build ./target/distributed-crawler-1.0-SNAPSHOT.jar ./distributed-crawler-1.0-SNAPSHOT.jar
COPY --from=build ./target/search-eng-1.0-SNAPSHOT.jar ./search-engine-1.0-SNAPSHOT.jar
EXPOSE 3000
#ENTRYPOINT ["java", "-DFRONTIER_QUEUE_NAME=url-frontier", "-DINDEX_QUEUE_NAME=index-queue","-DDOCUMENT_BUCKET_NAME=555documents", "-DREDIS_HOST=localhost", "-jar","./distributed-crawler-1.0-SNAPSHOT.jar"]
ENTRYPOINT ["java", "-jar","./search-eng-1.0-SNAPSHOT.jar"]