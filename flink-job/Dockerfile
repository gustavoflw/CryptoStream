# Stage 1 — build fat JAR
FROM eclipse-temurin:17-jdk-jammy AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN curl -fL "https://github.com/sbt/sbt/releases/download/v1.10.2/sbt-1.10.2.tgz" \
    | tar xz -C /usr/local
ENV PATH="/usr/local/sbt/bin:$PATH"

# Cache dependency resolution separately from source
COPY build.sbt ./
COPY project/ project/
RUN sbt update

COPY src/ src/
RUN sbt assembly

# Stage 2 — Flink runtime
FROM flink:1.20-java17
COPY --from=builder /app/target/scala-2.12/flink-job-assembly-0.1.0.jar /opt/flink/usrlib/job.jar
