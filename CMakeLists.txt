cmake_minimum_required(VERSION 3.23)
project(LuaMySQLBinding)

set(CMAKE_CXX_STANDARD 23)

include_directories(Dependencies\\Lua\\)
include_directories(Dependencies\\Include\\)
include_directories(C:\\LuaJIT-2.1.M.64\\src)
include_directories(D:\\LuaXE\\shared\\include)

add_library(LuaMySQLBinding SHARED library.cpp dll.def)

find_library(LUAJ lua51dyn lib C:\\LuaJIT-2.1.M.64\\src\\)

target_link_libraries(LuaMySQLBinding LINK_PUBLIC ${LUAJ})

find_library(MYSQL641 libcrypto lib Dependencies/Lib/)
find_library(MYSQL642 libssl lib Dependencies/Lib/)
find_library(MYSQL643 mysqlcppconn lib Dependencies/Lib/)
find_library(MYSQL644 mysqlcppconn8 lib Dependencies/Lib/)

target_link_libraries(LuaMySQLBinding LINK_PUBLIC ${LUA51} ${MYSQL641} ${MYSQL642} ${MYSQL643} ${MYSQL644})

set_property(TARGET LuaMySQLBinding PROPERTY MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
add_compile_options(
        $<$<CONFIG:>:/MT> #---------|
        $<$<CONFIG:Debug>:/MTd> #---|-- Statically link the runtime libraries
        $<$<CONFIG:Release>:/MT> #--|
)

target_compile_options(LuaMySQLBinding PRIVATE "/Zi" PRIVATE "/O2" PRIVATE "/showIncludes")
target_link_options(LuaMySQLBinding PRIVATE "/nologo" PRIVATE "/OPT:REF" PRIVATE "/OPT:ICF" PRIVATE "/INCREMENTAL" PRIVATE "/MACHINE:X64" PRIVATE "/LARGEADDRESSAWARE" PRIVATE "/LTCG")
target_link_options(LuaMySQLBinding PRIVATE "/DEBUG")
target_link_options(LuaMySQLBinding PRIVATE "/OPT:REF")
target_link_options(LuaMySQLBinding PRIVATE "/OPT:ICF")

add_custom_command(TARGET LuaMySQLBinding POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy LuaMySQLBinding.dll "mysql.dll")
