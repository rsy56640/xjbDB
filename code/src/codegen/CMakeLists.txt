CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT (query)

INCLUDE_DIRECTORIES(
    ../include
)

ADD_COMPILE_OPTIONS(
    -mavx2
    -march=broadwell
    -fPIC
    -shared
    -L../../build
    -lap_exec
    -Wl,-rpath=../../build
)

ADD_DEFINITIONS(-std=c++17)

SET(LIBRARY_OUTPUT_PATH "../../build")

SET(QUERY_SRC ./query.cpp)
ADD_LIBRARY(query SHARED ${QUERY_SRC})
TARGET_LINK_LIBRARIES(query ap_exec)
TARGET_LINK_LIBRARIES(query pthread)