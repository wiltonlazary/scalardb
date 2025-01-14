FROM clojure:openjdk-8-lein-slim-buster AS BUILD
COPY . /code
WORKDIR /code
RUN lein uberjar

FROM openjdk:8u275-jre-slim

# Fix CVE-2021-3520, CVE-2021-33560, CVE-2021-20231, CVE-2021-20232, CVE-2020-24659, sCVE-2021-20305 and CVE-2021-23840
RUN apt-get update && apt-get install -y --no-install-recommends \
    liblz4-1=1.8.3-1+deb10u1 \
    libgcrypt20=1.8.4-5+deb10u1 \
    libgnutls30=3.6.7-4+deb10u7 \
    libhogweed4=3.4.1-1+deb10u1 \
    libnettle6=3.4.1-1+deb10u1 \
    libssl1.1=1.1.1d-0+deb10u6 \
    openssl=1.1.1d-0+deb10u6 \
    && rm -rf /var/lib/apt/lists/* \

WORKDIR /app
COPY --from=BUILD /code/target/scalar-schema-standalone-*.jar ./app.jar

RUN groupadd -r --gid 201 scalar && \
    useradd -r --uid 201 -g scalar scalar

USER 201

ENTRYPOINT ["java", "-jar", "app.jar"]

