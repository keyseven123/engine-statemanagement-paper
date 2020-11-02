#ifndef NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_
#define NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_

#include <cstdint>
#include <vector>
#include <list>
#include <random>
#include <NodeEngine/QueryStatistics.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <API/Query.hpp>
#include <Version/version.hpp>

namespace NES::Benchmarking{
/**
 * @brief This class provides several helper functions for creating benchmarks.
 */
class BenchmarkUtils {
  public:
    static uint64_t runSingleExperimentSeconds;
    static uint64_t periodLengthInSeconds;

    /**
    * @brief creates a vector with a range of [start, stop) and step size
    */
    static void createRangeVector(std::vector<uint64_t>& vector, uint64_t start, uint64_t stop, uint64_t stepSize);


    /**
     * @brief creates a list with values drawn from an uniform distribution of the range [0,999]. The list size is totalNumberOfTuples
     * @param dataList
     * @param totalNumberOfTuples
     */
    static void createUniformData(std::list<uint64_t>& dataList, uint64_t totalNumberOfTuples);

    /**
     * @brief This function calculates the cardinality of a simple selection with a given selectivity.
     * @param vector Expects a vector with values in the range of [0,999] as they are used for the selectivity
     * @param selectivity
     * @return
     */
    static uint64_t calcExpectedTuplesSelectivity(std::list<uint64_t> list, uint64_t selectivity);

    /**
     * @brief saves every second statistics from QueryStatistics into statisticsVec
     * @param statisticsVec
     * @param nodeEngine
     */
    static void recordStatistics(std::vector<QueryStatistics*>& statisticsVec, NodeEnginePtr nodeEngine);

    /**
     * @brief computes difference between each vector item of its predecessor. Also deletes statisticsVec[0]
     * @param statisticsVec
     */
    static void computeDifferenceOfStatistics(std::vector<QueryStatistics*>& statisticsVec);

    /**
     * @return string with format {dateTime}_v{current NES_VERSION}
     */
    static std::string getCurDateTimeStringWithNESVersion();

    /**
     * @brief ProcessedBytes is calculated by ProcessedTuples * SchemaSizeInBytes
     * @param statistic
     * @param schema
     * @return comma seperated string {ProcessedBuffers},{ProcessedTasks},{ProcessedTuples},{ProcessedBytes}
     */
    static std::string getStatisticsAsCSV(QueryStatistics* statistic, SchemaPtr schema);


    /**
     * @brief runs a benchmark with the given ingestion rate, given query, and a benchmark schema. The statistics (processedTuples)
     * of this benchmark run are also saved.
     * @param benchmarkSchema
     * @param statisticsVec
     * @param query
     * @param ingestionRate
     */
    static void runBenchmark(std::vector<NES::QueryStatistics*>& statisticsVec,
                             NES::DataSourcePtr benchmarkSource,
                             NES::DataSinkPtr benchmarkSink,
                             NES::NodeEnginePtr nodeEngine,
                             NES::Query query,
                             uint64_t workerThreads);

};

/**
 * @brief BM_AddBenchmark helps creating a new benchmark by providing a solid building block.
 * It requires std::vectors of type uint64_t named {allIngestionRates, allExperimentsDuration, allPeriodLengths}
 * @param workerThreads is currently not supported and should always be set to 1
 */
#define BM_AddBenchmark(benchmarkName, benchmarkQuery, workerThreads, benchmarkSource, benchmarkSink, csvHeaderString, customCSVOutputs) { \
NES::setupLogging(benchmarkFolderName + "/" + (benchmarkName) + ".log", NES::LOG_WARNING);\
\
    try{                                                                                                                                   \
        std::ofstream benchmarkFile;\
        benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv", std::ios_base::app);\
        benchmarkFile << "BM_Name,NES_Version,Ingestionrate,WorkerThreads,RunSingleExperiment,PeriodLength,ProcessedBuffers,ProcessedTasks,ProcessedTuples,ProcessedBytes" << (csvHeaderString) << "\n";\
        benchmarkFile.close();                                                                                                             \
                                                                                                                                           \
        for (auto ingestionRate : allIngestionRates){\
            for (auto experimentDuration : allExperimentsDuration){\
                for (auto periodLength : allPeriodLengths) {                                                                               \
                                                                                                                                           \
                    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();\
                    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);;                                                                  \
                    \
                    BenchmarkUtils::runSingleExperimentSeconds = experimentDuration;\
                    BenchmarkUtils::periodLengthInSeconds = periodLength;\
    \
                    std::vector<QueryStatistics*> statisticsVec;\
                    NES_WARNING("Starting benchmark with " + std::to_string(ingestionRate)\
                                    + ", " + std::to_string(BenchmarkUtils::runSingleExperimentSeconds)\
                                    + ", " + std::to_string(BenchmarkUtils::periodLengthInSeconds));\
                    DataSourcePtr thisSource = (benchmarkSource);                                                                                 \
                    DataSinkPtr thisSink = (benchmarkSink);\
                    SchemaPtr thisSchema = (benchmarkSchema);\
                    Query thisQuery = (benchmarkQuery);\
                    BenchmarkUtils::runBenchmark(statisticsVec, thisSource, thisSink, nodeEngine, thisQuery, workerThreads);\
    \
                    benchmarkFile.open(benchmarkFolderName + "/" + (benchmarkName) + "_results.csv",\
                                       std::ios_base::app);\
    \
                    for (auto statistic : statisticsVec) {\
                        benchmarkFile << (benchmarkName)                                                                                   \
                                      << ",\"" << NES_VERSION << "\""                                                                                        \
                                      << "," << std::to_string(ingestionRate)                                                              \
                                      << "," << std::to_string(workerThreads)\
                                      << "," << std::to_string(BenchmarkUtils::runSingleExperimentSeconds)\
                                      << "," << std::to_string(BenchmarkUtils::periodLengthInSeconds)\
                                      << BenchmarkUtils::getStatisticsAsCSV(statistic, thisSchema)                                                     \
                                      << (customCSVOutputs)\
                                      << "\n";\
    \
                        delete statistic;\
                    }\
    \
                    benchmarkFile.close();\
                }\
            }                                                                                                                              \
        }                                                                                                                                       \
    } catch (RuntimeException& e) {\
        NES_ERROR("Caught RuntimeException: " << e.what());\
    }\
\
    NES::NESLogger->removeAllAppenders();\
}

#define printPIDandParentID (std::cout <<  __FUNCTION__ << " called by process " << ::getpid() << " (parent: " << ::getppid() << ")" << std::endl)
}

#endif //NES_BENCHMARK_UTIL_BENCHMARKUTILS_HPP_
