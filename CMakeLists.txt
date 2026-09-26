set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
cmake_minimum_required(VERSION 3.23)
project(Nightmare)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_executable(Nightmare
	src/main.cpp	
)

target_link_libraries(Nightmare
	PRIVATE
		Boost::boost
		Threads::Threads
)
