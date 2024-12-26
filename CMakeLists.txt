cmake_minimum_required(VERSION 3.29)
project(stock_exchange)
set(CMAKE_CXX_STANDARD 20)

find_package(nlohmann_json 3.11.3 REQUIRED)
find_package(Boost 1.74.0 REQUIRED COMPONENTS system filesystem)

add_executable(stock_exchange main.cpp)
target_link_libraries(stock_exchange PRIVATE nlohmann_json::nlohmann_json Boost::system Boost::filesystem)