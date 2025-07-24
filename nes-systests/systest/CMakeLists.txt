# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
add_subdirectory(checksum)

# Include necessary modules
include(${CMAKE_SOURCE_DIR}/cmake/CodeCoverage.cmake)

# This uses ExternalData to manage test data for the system tests.
# Files can either be stored directly in the testdata/small folder, but this is only permitted for small files or
# referenced via a hash an fetched from a remote server on demand. Large files have to be uploaded to bench.nebula.stream/data (ask a maintainer)!
include(ExternalData)
set(ExternalData_URL_TEMPLATES
        "https://tubcloud.tu-berlin.de/s/28Tr2wTd73Ggeed/download?files=%(algo)_%(hash)"
)
if (ENABLE_LARGE_TESTS)
    set(TEST_DATA_PATH DATA{../testdata/,RECURSE:,REGEX:.*})
else ()
    # By default this only includes small files located in testdata/small
    set(TEST_DATA_PATH DATA{../testdata/,RECURSE:,REGEX:small.*})
endif ()
set(ExternalData_TIMEOUT_ABSOLUTE 4800)
ExternalData_Expand_Arguments(
        test-data
        EXPANDED_TEST_DATA_PATH
        ${TEST_DATA_PATH}
)
message(STATUS "Systest testdata is located at ${EXPANDED_TEST_DATA_PATH}")

get_source(nes-systest-lib SLT_SOURCE_FILES)

add_library(nes-systest-lib STATIC ${SLT_SOURCE_FILES})
target_link_libraries(nes-systest-lib PUBLIC nes-nebuli-embedded-lib nes-single-node-worker-lib nes-sql-parser nes-systest-sources PRIVATE nes-input-formatters)
target_include_directories(nes-systest-lib PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_compile_definitions(nes-systest-lib PUBLIC
        PATH_TO_BINARY_DIR="${CMAKE_BINARY_DIR}"
        TEST_DISCOVER_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../"
        SYSTEST_EXTERNAL_DATA_DIR="${EXPANDED_TEST_DATA_PATH}"
        TEST_CONFIGURATION_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../configs"
)

# Find required packages
find_package(argparse CONFIG REQUIRED)

# Add the systest executable
add_executable(systest src/SystestStarter.cpp)
set_property(TARGET systest PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(systest PUBLIC nes-systest-lib argparse::argparse)
target_include_directories(systest PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>)

# We want to run different systests with and w/o slice caches for different no. entries
# We simply run the tests for LRU and SECOND_CHANCE, as they have showcased the best trade-off between accuracy and throughput
set(sliceCaches NONE LRU SECOND_CHANCE)
set(resourceAssignmentsFixQueues WORK_STEALING) #set(resourceAssignmentsFixQueues WORK_STEALING WORK_DEALING_ROUND_ROBIN)
set(resourceAssignmentsDynamicQueues WORK_DEALING_NEW_QUEUE_AND_THREAD)
set(resourceAssignmentCombined ${resourceAssignmentsDynamicQueues} ${resourceAssignmentsFixQueues})

# Add code coverage if enabled
if (CODE_COVERAGE)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc --data ${EXPANDED_TEST_DATA_PATH} -- --worker.defaultQueryExecution.executionMode=INTERPRETER --worker.queryEngine.taskQueueSize=100000)
    target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_Hash_Join PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_cc --data ${EXPANDED_TEST_DATA_PATH} -- --worker.defaultQueryExecution.executionMode=INTERPRETER --worker.queryEngine.taskQueueSize=100000 --worker.defaultQueryExecution.joinStrategy=HASH_JOIN)
    # Make sure to fetch ExternalData before running the ccov targets
    add_dependencies(ccov-run-systest_interpreter_coverage test-data)
    add_dependencies(ccov-run-systest_interpreter_coverage_Hash_Join test-data)

    foreach (resourceAssignment IN LISTS resourceAssignmentCombined)
        foreach (sliceCache IN LISTS sliceCaches)
            target_code_coverage(systest COVERAGE_TARGET_NAME systest_interpreter_coverage_${sliceCache}_${resourceAssignment} PUBLIC AUTO ALL ARGS -n 20 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_${sliceCache}_cc --data ${TEST_DATA_PATH} -- --worker.queryOptimizer.executionMode=INTERPRETER --worker.queryEngine.taskQueueSize=100000 --worker.queryEngine.resourceAssignment=${resourceAssignment} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
            # Make sure to fetch ExternalData before running the ccov targets
            add_dependencies(ccov-run-systest_interpreter_coverage_${sliceCache} TEST_DATA)
        endforeach ()
    endforeach ()
endif (CODE_COVERAGE)

# If we are running code coverage, we need to ONLY run the interpreter tests, as otherwise, the code coverage will be 100% for all operators as the compiler traces all branches and operations.
set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
foreach (resourceAssignment IN LISTS resourceAssignmentCombined)
    foreach (joinStrategy IN LISTS joinStrategies)
        ExternalData_Add_Test(TEST_DATA
                NAME systest_interpreter_${joinStrategy}_${resourceAssignment}
                COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/interpreter_${joinStrategy}_${resourceAssignment} --exclude-groups large --data ${SMALL_DATA} -- --worker.queryOptimizer.executionMode=INTERPRETER --worker.queryEngine.taskQueueSize=100000 --worker.queryEngine.resourceAssignment=${resourceAssignment} --worker.queryOptimizer.joinStrategy=${joinStrategy})
    endforeach ()
endforeach ()
if (NOT CODE_COVERAGE)
    ExternalData_Add_Test(test-data
            NAME systest_compiler
            COMMAND systest -n 20 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/compiler --exclude-groups large --data ${EXPANDED_TEST_DATA_PATH} -- --worker.defaultQueryExecution.executionMode=COMPILER --worker.queryEngine.taskQueueSize=100000)
endif (NOT CODE_COVERAGE)


if (ENABLE_LARGE_TESTS)
    message(STATUS "Large scale tests enabled")
    set(numberOfEntriesSliceCaches 10 100) # We want to reduce the no. large scale tests
    foreach (resourceAssignment IN LISTS resourceAssignmentCombined)
        foreach (sliceCache IN LISTS sliceCaches)
            foreach (cacheSize IN LISTS numberOfEntriesSliceCaches)
                if (sliceCache STREQUAL "NONE")
                    if (NOT cacheSize EQUAL 10)
                        message(STATUS "Skipping ${cacheSize} for ${sliceCache}, as we do not use a slice cache")
                        continue()
                    endif()
                endif()

                set(joinStrategy HASH_JOIN) # We simp<ly test here with a hash join, as a NLJ does not finish in 2h
                ExternalData_Add_Test(TEST_DATA_LARGE
                        NAME systest_large_${joinStrategy}_${sliceCache}_${cacheSize}_${resourceAssignment}
                        COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy}_${sliceCache}_${cacheSize}_${resourceAssignment} --data ${LARGE_DATA} --groups large File
                        --exclude-groups Memory -- --worker.queryOptimizer.executionMode=COMPILER --worker.queryEngine.resourceAssignment=${resourceAssignment} --worker.queryEngine.numberOfWorkerThreads=8 --worker.numberOfBuffersInGlobalBufferManager=8000000 --worker.queryOptimizer.joinStrategy=${joinStrategy} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})

                # Running here the Nexmark.test queries with NLJ to test it for larger data sets but the input data size
                # is still small enough to finish the test in minutes.
                set(joinStrategy NESTED_LOOP_JOIN)
                ExternalData_Add_Test(TEST_DATA_LARGE
                        NAME systest_large_${joinStrategy}_${sliceCache}_${cacheSize}_${resourceAssignment}
                        COMMAND systest -n 6 --workingDir=${CMAKE_CURRENT_BINARY_DIR}/large_scale_${joinStrategy}_${sliceCache}_${cacheSize}_${resourceAssignment} --data ${LARGE_DATA} --groups NestedLoopJoin --exclude-groups Memory -- --worker.queryOptimizer.executionMode=COMPILER --worker.queryEngine.resourceAssignment=${resourceAssignment} --worker.queryEngine.numberOfWorkerThreads=8 --worker.numberOfBuffersInGlobalBufferManager=8000000 --worker.queryOptimizer.joinStrategy=${joinStrategy} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
            endforeach ()
        endforeach ()
    endforeach ()
endif ()


if (NOT CODE_COVERAGE)
    # We run all tests with some small and one large task queue size
    # We set the number of worker threads to 8, to further stress the task queue
    set(taskQueueSize 100 10000)
    foreach (taskQueueSize IN LISTS taskQueueSize)
        foreach (sliceCache IN LISTS sliceCaches)
            foreach (cacheSize IN LISTS numberOfEntriesSliceCaches)
                if (sliceCache STREQUAL "NONE")
                    if (NOT cacheSize EQUAL 10)
                        message(STATUS "Skipping ${cacheSize} for ${sliceCache}, as we do not use a slice cache")
                        continue()
                    endif()
                endif()
                ExternalData_Add_Test(test-data
                        NAME systest_task_queue_size_${taskQueueSize}_${sliceCache}_${cacheSize}_interpreter
                        COMMAND systest -n 4 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_${sliceCache}_${cacheSize}_interpreter --data ${EXPANDED_TEST_DATA_PATH} -- --worker.queryEngine.numberOfWorkerThreads=4 --worker.defaultQueryExecution.executionMode=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=200000 --worker.queryEngine.taskQueueSize=${taskQueueSize} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
                ExternalData_Add_Test(test-data
                        NAME systest_task_queue_size_${taskQueueSize}_${sliceCache}_${cacheSize}_compiler
                        COMMAND systest -n 4 --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${taskQueueSize}_${sliceCache}_${cacheSize}_compiler --data ${EXPANDED_TEST_DATA_PATH} -- --worker.queryEngine.numberOfWorkerThreads=4 --worker.defaultQueryExecution.executionMode=COMPILER --worker.numberOfBuffersInGlobalBufferManager=200000 --worker.queryEngine.taskQueueSize=${taskQueueSize} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
            endforeach ()
        endforeach ()
    endforeach ()

    ## We run all join and aggregation tests with different no. worker threads and different join strategies
    set(workerThreads 1 2 4 8)
    set(numberOfEntriesSliceCaches 2 10 100)
    set(joinStrategies NESTED_LOOP_JOIN HASH_JOIN)
    foreach (workerThreads IN LISTS workerThreads)
        foreach (sliceCache IN LISTS sliceCaches)
            foreach (cacheSize IN LISTS numberOfEntriesSliceCaches)
                if (sliceCache STREQUAL "NONE")
                    if (NOT cacheSize EQUAL 10)
                        message(STATUS "Skipping ${cacheSize} for ${sliceCache}, as we do not use a slice cache")
                        continue()
                    endif()
                endif()
                foreach (joinStrategy IN LISTS joinStrategies)
                    ExternalData_Add_Test(TEST_DATA
                            NAME systest_join_${workerThreads}_${joinStrategy}_${sliceCache}_${cacheSize}_interpreter
                            COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_${sliceCache}_${cacheSize}_interpreter_join --data ${SMALL_DATA}
                            --
                            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryOptimizer.executionMode=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryOptimizer.joinStrategy=${joinStrategy} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
                    ExternalData_Add_Test(TEST_DATA
                            NAME systest_join_${workerThreads}_${joinStrategy}_${sliceCache}_${cacheSize}_compiler
                            COMMAND systest -n 6 --groups Join --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${joinStrategy}_${sliceCache}_${cacheSize}_compiler_join --data ${SMALL_DATA}
                            --
                            --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryOptimizer.executionMode=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryOptimizer.joinStrategy=${joinStrategy} --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
                endforeach ()
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_agg_${workerThreads}_${sliceCache}_${cacheSize}_interpreter
                        COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${sliceCache}_${cacheSize}_interpreter_aggregation --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryOptimizer.executionMode=INTERPRETER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})
                ExternalData_Add_Test(TEST_DATA
                        NAME systest_agg_${workerThreads}_${sliceCache}_${cacheSize}_compiler
                        COMMAND systest -n 6 --groups Aggregation --exclude-groups large --workingDir=${CMAKE_CURRENT_BINARY_DIR}/${workerThreads}_${sliceCache}_${cacheSize}_compiler_aggregation --data ${SMALL_DATA}
                        --
                        --worker.queryEngine.numberOfWorkerThreads=${workerThreads} --worker.queryOptimizer.executionMode=COMPILER --worker.numberOfBuffersInGlobalBufferManager=20000 --worker.queryOptimizer.sliceCache.numberOfEntriesSliceCache=${cacheSize} --worker.queryOptimizer.sliceCache.sliceCacheType=${sliceCache})

            endforeach ()
        endforeach ()
    endforeach ()
endif (NOT CODE_COVERAGE)

# Adding dependency for the nes-systest-lib so that the data is downloaded before the tests are run
ExternalData_Add_Target(test-data)
add_dependencies(nes-systest-lib test-data)

add_tests_if_enabled(tests)
