cmake_minimum_required(VERSION 3.10)

# Set the project name and version
project(FlintKV VERSION 1.0)

# Specify the C++ standard required for std::optional and other features
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. Create the Static Library
# We include the header files so they appear in IDEs, though they aren't "compiled" 
# into object files in a traditional sense since they contain the full implementation.
add_library(flintkv STATIC 
    BPlusTree.h 
    BufferPool.h 
    Page.h
)

# 2. Force the Linker Language
# This fixes the "Cannot determine link language" error by telling CMake 
# to treat this as a C++ project even without a .cpp file in this target.
set_target_properties(flintkv PROPERTIES LINKER_LANGUAGE CXX)

# 3. Set Include Directories
# This allows users of your library to include headers via #include "BPlusTree.h"
target_include_directories(flintkv PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# 4. Installation rules (Optional)
# This allows you to run 'make install' to move the library and headers to a system folder
install(TARGETS flintkv DESTINATION lib)
install(FILES BPlusTree.h BufferPool.h Page.h DESTINATION include)