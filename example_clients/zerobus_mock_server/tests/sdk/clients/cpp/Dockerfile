# SDK matrix client: C++. The full rust:1.94-bookworm image provides cargo, g++ and make (the slim
# image has no g++). The SDK is built from the monorepo by build.sh.
FROM rust:1.94-bookworm
WORKDIR /client
COPY build.sh run.sh client.cpp vendor.py fixtures.py /client/
RUN chmod +x /client/build.sh /client/run.sh
ENTRYPOINT ["/client/run.sh"]
