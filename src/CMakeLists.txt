#writer by shuntan@Tencent.com on 2017-02-28

cmake_minimum_required(VERSION 2.8)
project(hbase_client)

# 设置zookeeper官方库安装目录
set(ZOOKEEPER_HOME /usr/local/thirdparty/zookeeper)

# 编译参数
# 启用__STDC_FORMAT_MACROS是为了可以使用inttypes.h中的PRId64等
# 启用__STDC_LIMIT_MACROS是为了可以使用stdint.h中的__UINT64_C和INT32_MIN等
add_definitions("-Wall -fPIC -pthread -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -Wno-deprecated")

# 设置依赖库路径
#zookeeper库
include_directories(${ZOOKEEPER_HOME}/include)
link_directories(${ZOOKEEPER_HOME}/lib)

link_libraries(dl pthread rt z)

# 生成 Zookeeper helper 库
link_directories(${CMAKE_CURRENT_SOURCE_DIR})
set(ZOOKEEPER_SRC zookeeper_helper.cpp)
add_library(zookeeper_helper STATIC ${ZOOKEEPER_SRC})

# 使用例子,编译时默认使用libzookeeper_mt.a -- 多线程版本库
add_executable(client_test client_test.cpp)
target_link_libraries(client_test libzookeeper_helper.a libzookeeper_mt.a)    

add_executable(configure_test configure_test.cpp)
target_link_libraries(configure_test libzookeeper_helper.a libzookeeper_mt.a)  

# 设置依赖关系：hbase_client_helper 库需要最先被编译
ADD_DEPENDENCIES(client_test zookeeper_helper)

# CMAKE_INSTALL_PREFIX
install(
        TARGETS zookeeper_helper
        DESTINATION lib
       )