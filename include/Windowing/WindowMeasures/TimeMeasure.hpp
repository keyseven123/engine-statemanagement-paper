#ifndef NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
#include <Windowing/WindowMeasures/WindowMeasure.hpp>
#include <cstdint>
namespace NES::Windowing {

/**
 * A time based window measure.
 */
class TimeMeasure : public WindowMeasure {
  public:
    explicit TimeMeasure(uint64_t ms);

    /**
     * @brief gets the time measure in milliseconds
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getTime() const;

  private:
    const uint64_t ms;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWMEASURES_TIMEMEASURE_HPP_
