FROM mariadb:10.6
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install mariadb-plugin-rocksdb && rm -rf /var/cache/apt/lists/*
RUN mkdir -p /etc/mysql
COPY ./contrib/mariadb/config/my.cnf /etc/mysql/my.cnf
RUN chmod 644 /etc/mysql/my.cnf
