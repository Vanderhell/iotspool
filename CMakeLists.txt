cmake_minimum_required(VERSION 3.16)
project(iotspool VERSION 0.1.0 LANGUAGES C)

set(IOTSPOOL_C_STANDARD "99" CACHE STRING "C language standard for iotspool builds")
set_property(CACHE IOTSPOOL_C_STANDARD PROPERTY STRINGS 99 11)
set(CMAKE_C_STANDARD "${IOTSPOOL_C_STANDARD}")
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(IOTSPOOL_BUILD_TESTS   "Build test suite"      ON)
option(IOTSPOOL_BUILD_EXAMPLES "Build host examples"  ON)
option(IOTSPOOL_ENABLE_SHA256  "Enable SHA-256 support" ON)

# ── Warnings ────────────────────────────────────────────────────────────────
if(MSVC)
    add_compile_options(/W4 /WX)
elseif(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -Werror
                        -Wconversion -Wsign-conversion -Wshadow -Wformat=2 -Wundef)
endif()

# ── Library ─────────────────────────────────────────────────────────────────
add_library(iotspool STATIC
    src/spool.c
    src/record.c
    src/crc32.c
    src/backoff.c
    src/store_posix.c
)
target_include_directories(iotspool PUBLIC include)
target_include_directories(iotspool PRIVATE src)

if(MSVC)
    target_compile_definitions(iotspool PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(IOTSPOOL_ENABLE_SHA256)
    target_compile_definitions(iotspool PUBLIC IOTSPOOL_ENABLE_SHA256)
    target_sources(iotspool PRIVATE src/sha256.c)
endif()

# ── Tests ────────────────────────────────────────────────────────────────────
if(IOTSPOOL_BUILD_TESTS)
    enable_testing()
    add_executable(iotspool_tests tests/test_main.c)
    target_link_libraries(iotspool_tests PRIVATE iotspool)
    target_include_directories(iotspool_tests PRIVATE src include)
    add_test(NAME iotspool_tests COMMAND $<TARGET_FILE:iotspool_tests>)
endif()

# ── Examples ─────────────────────────────────────────────────────────────────
if(IOTSPOOL_BUILD_EXAMPLES AND NOT WIN32 AND NOT CMAKE_CROSSCOMPILING)
    add_executable(example_rpi examples/rpi/main.c)
    target_link_libraries(example_rpi PRIVATE iotspool)
endif()

if(IOTSPOOL_BUILD_EXAMPLES AND CMAKE_CROSSCOMPILING)
    add_executable(iotspool_arm_smoke examples/arm_smoke.c)
    target_link_libraries(iotspool_arm_smoke PRIVATE iotspool)
endif()
