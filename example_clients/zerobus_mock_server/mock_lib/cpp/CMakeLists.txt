# UNOFFICIAL: a community test fake of the Zerobus C++ SDK (zerobus::Sdk, zerobus::Stream). Not affiliated with Databricks.
cmake_minimum_required(VERSION 3.16)

project(
  zerobus_mock
  VERSION 0.1.0
  DESCRIPTION "UNOFFICIAL: in-process fake of the Zerobus C++ SDK for unit tests (no network). Not affiliated with Databricks."
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(zerobus_mock STATIC src/zerobus_mock.cpp)
target_include_directories(zerobus_mock PUBLIC include)
target_compile_options(zerobus_mock PRIVATE -Wall -Wextra -Wpedantic -Werror)

# Test dependencies come from the Go module proxy (the container cannot reach github.com directly on
# every host). Each archive is pinned by SHA-256. GoogleTest v1.15.2; nlohmann/json v3.11.3.
set(ZBMOCK_GOPROXY "https://proxy.golang.org" CACHE STRING "Go module proxy base URL (no trailing slash)")
include(FetchContent)
FetchContent_Declare(
  googletest
  URL "${ZBMOCK_GOPROXY}/github.com/google/googletest/@v/v1.15.2.zip"
  URL_HASH SHA256=2445790c4a54cc4e563e179b34816400115f440e746d0399d5c78151141b3267
  SOURCE_SUBDIR google/googletest@v1.15.2)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
set(JSON_Install OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  nlohmann_json
  URL "${ZBMOCK_GOPROXY}/github.com/nlohmann/json/@v/v3.11.3+incompatible.zip"
  URL_HASH SHA256=c4559ac5b850cc48ab475e442931afca9edc992055e1a0501f990735164e2c43
  SOURCE_SUBDIR nlohmann/json@v3.11.3+incompatible)
FetchContent_MakeAvailable(nlohmann_json)

enable_testing()
add_executable(zerobus_mock_test tests/zerobus_mock_test.cpp)
target_compile_options(zerobus_mock_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
target_compile_definitions(zerobus_mock_test PRIVATE
  "ZBMOCK_SCENARIOS_JSON=\"${CMAKE_CURRENT_SOURCE_DIR}/../scenarios.json\"")
target_link_libraries(zerobus_mock_test PRIVATE zerobus_mock gtest_main nlohmann_json)

add_test(NAME zerobus_mock COMMAND zerobus_mock_test)
