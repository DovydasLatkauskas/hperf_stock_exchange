cmake_minimum_required(VERSION 3.29)
project(stock_exchange)
set(CMAKE_CXX_STANDARD 20)

find_package(Boost 1.74.0 REQUIRED COMPONENTS system filesystem)

find_package(CapnProto REQUIRED)
include_directories(${CapnProto_INCLUDE_DIRS})

add_executable(stock_exchange main.cpp)
target_link_libraries(stock_exchange
        PRIVATE
        Boost::system
        Boost::filesystem
        CapnProto::capnp
        CapnProto::kj
)