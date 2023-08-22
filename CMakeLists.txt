# For more information about build system see
# https://docs.espressif.com/projects/esp-idf/en/latest/api-guides/build-system.html
# The following five lines of boilerplate have to be in your project's
# CMakeLists in this exact order for cmake to work correctly
cmake_minimum_required(VERSION 3.16)

include($ENV{IDF_PATH}/tools/cmake/project.cmake)
project(esp32-mesh-gateway)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror=format=0" CACHE STRING "" FORCE)

SET(CONFIG_NAME "test")

add_custom_target(config_bin
                  COMMAND python $ENV{IDF_PATH}/components/nvs_flash/nvs_partition_generator/nvs_partition_gen.py generate --version 2 --outdir ${CMAKE_BINARY_DIR} "cred.csv" config.bin 0x3000
                  WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/certs/${CONFIG_NAME})

add_dependencies(flash config_bin)

esptool_py_flash_to_partition(flash "config" ${CMAKE_BINARY_DIR}/config.bin)
