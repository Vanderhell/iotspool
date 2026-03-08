cmake_minimum_required(VERSION 3.16)
add_executable(iotspool_demo_rpi main.c)
target_link_libraries(iotspool_demo_rpi PRIVATE iotspool iotspool_store_posix)
target_include_directories(iotspool_demo_rpi PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)
