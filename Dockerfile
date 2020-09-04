FROM clojure:openjdk-11-lein
ADD . /app
WORKDIR /app

RUN apt-get update && \
  apt-get install -y netcat

