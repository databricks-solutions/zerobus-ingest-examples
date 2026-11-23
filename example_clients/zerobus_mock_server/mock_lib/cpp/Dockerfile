# C++ unit tests for the Zerobus mock lib (GoogleTest and nlohmann/json, pinned, built with CMake).
# Build context is the mock_lib directory.
#   docker build --network host -f cpp/Dockerfile -t zbmock-cpp:test .
#   docker run --rm --network host zbmock-cpp:test
# cmake comes from the pinned PyPI wheel: the gcc image has no cmake, and apt is not usable on every host.
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG GOPROXY=https://proxy.golang.org
FROM python:3.12-slim-bookworm AS cmake
ARG PIP_INDEX_URL
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
RUN pip install --no-cache-dir cmake==3.31.6

FROM gcc:14.2.0-bookworm
ARG GOPROXY
COPY --from=cmake /usr/local/lib/python3.12/site-packages/cmake/data /opt/cmake
ENV PATH=/opt/cmake/bin:${PATH}
WORKDIR /zbmock/cpp
COPY cpp/CMakeLists.txt ./
COPY cpp/include ./include
COPY cpp/src ./src
COPY cpp/tests ./tests
# The tests read ../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DZBMOCK_GOPROXY="${GOPROXY}" \
 && cmake --build build -j"$(nproc)"
CMD ["./build/zerobus_mock_test"]
