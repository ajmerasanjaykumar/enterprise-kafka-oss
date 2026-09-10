# Multi-stage Dockerfile for Maven build and JAR deployment
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests -B

FROM alpine:latest
WORKDIR /deploy
COPY --from=builder /app/target/alert-engine-flink-1.0.0.jar /deploy/alert-engine-flink.jar
CMD ["echo", "JAR build complete at /deploy/alert-engine-flink.jar"]
