FROM rust:latest

RUN set -ex;\
    sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list;\
    apt-get update; \
    apt-get install -y clang libclang-dev;
ADD . /src
WORKDIR /src
RUN set -ex;\
    cargo build --release
