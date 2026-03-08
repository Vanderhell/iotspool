cmake_minimum_required(VERSION 3.16)
project(iotspool VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

option(IOTSPOOL_BUILD_TESTS   "Build test suite"      ON)
option(IOTSPOOL_BUILD_EXAMPLES "Build host examples"  ON)
option(IOTSPOOL_ENABLE_SHA256  "Enable SHA-256 support" ON)

# ── Warnings ────────────────────────────────────────────────────────────────
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

# ── Library ─────────────────────────────────────────────────────────────────
add_library(iotspool STATIC
    src/spool.c
    src/record.c
    src/crc32.c
    src/sha256.c
    src/backoff.c
    src/store_posix.c
)
target_include_directories(iotspool PUBLIC include)
target_include_directories(iotspool PRIVATE src)

if(IOTSPOOL_ENABLE_SHA256)
    target_compile_definitions(iotspool PUBLIC IOTSPOOL_ENABLE_SHA256)
endif()

# ── Tests ────────────────────────────────────────────────────────────────────
if(IOTSPOOL_BUILD_TESTS)
    enable_testing()
    add_executable(iotspool_tests tests/test_main.c)
    target_link_libraries(iotspool_tests PRIVATE iotspool)
    target_include_directories(iotspool_tests PRIVATE src)
    add_test(NAME iotspool_tests COMMAND iotspool_tests)
endif()

# ── Examples ─────────────────────────────────────────────────────────────────
if(IOTSPOOL_BUILD_EXAMPLES)
    add_executable(example_rpi examples/rpi/main.c)
    target_link_libraries(example_rpi PRIVATE iotspool)
endif()
