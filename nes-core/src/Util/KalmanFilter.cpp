/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sensors/Values/SingleSensor.hpp>
#include <Util/KalmanFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <ctime>

namespace NES {
KalmanFilter::KalmanFilter(const uint64_t errorWindowSize)
    : kfErrorWindow(errorWindowSize), lastValuesWindow(2) {
        this->calculateTotalEstimationErrorDivider(errorWindowSize);
};

KalmanFilter::KalmanFilter(double timeStep,
                           const Eigen::MatrixXd F,
                           const Eigen::MatrixXd H,
                           const Eigen::MatrixXd Q,
                           const Eigen::MatrixXd R,
                           const Eigen::MatrixXd P,
                           const uint64_t errorWindowSize)
    : m(H.rows()), n(F.rows()), stateTransitionModel(F), observationModel(H), processNoiseCovariance(Q), measurementNoiseCovariance(R), initialEstimateCovariance(P), identityMatrix(n, n), xHat(n), xHatNew(n), innovationError(n),
      timeStep(timeStep), kfErrorWindow(errorWindowSize), lastValuesWindow(2) {
    this->calculateTotalEstimationErrorDivider(errorWindowSize);
    identityMatrix.setIdentity();
}

void KalmanFilter::init() {
    this->setDefaultValues();
    this->xHat.setZero();
}

void KalmanFilter::init(const Eigen::VectorXd& initialState) {
    this->setDefaultValues();
    this->xHat = initialState;
    this->lastValuesWindow.emplace(initialState[initialState.size() - 1]);
}

void KalmanFilter::init(const Eigen::VectorXd& initialState, double initialTimestamp) {
    this->setDefaultValues();
    this->xHat = initialState;
    this->initialTimestamp = initialTimestamp;
    this->currentTime = initialTimestamp;
}

void KalmanFilter::setDefaultValues() {

    // measurements
    this->m = 1;
    // states
    this->n = 3;
    // timestep value
    this->timeStep = 1.0 / 30;

    // initialize system dymanics and observation matrices
    this->stateTransitionModel = Eigen::MatrixXd(this->n, this->n);
    this->observationModel = Eigen::MatrixXd(this->m, this->n);
    this->processNoiseCovariance = Eigen::MatrixXd(this->n, this->n);
    this->measurementNoiseCovariance = Eigen::MatrixXd(this->m, this->m);
    this->initialEstimateCovariance = Eigen::MatrixXd(this->n, this->n);

    // Discrete LTI projectile motion, measuring position only
    this->stateTransitionModel << 1, this->timeStep, 0, 0, 1, this->timeStep, 0, 0, 1;
    this->observationModel << 1, 0, 0;

    // Reasonable covariance matrices
    this->processNoiseCovariance << .05, .05, .0, .05, .05, .0, .0, .0, .0;
    this->measurementNoiseCovariance << 5;
    this->initialEstimateCovariance << .1, .1, .1, .1, 10000, 10, .1, 10, 100;

    // rest of initializations
    this->estimateCovariance = this->initialEstimateCovariance;
    this->identityMatrix = Eigen::MatrixXd(this->n, this->n);
    this->identityMatrix.setIdentity();
    this->xHat = Eigen::VectorXd(this->n);
    this->xHatNew = Eigen::VectorXd(this->n);
    this->innovationError = Eigen::VectorXd(this->n);
    this->initialTimestamp = std::time(nullptr);
    this->currentTime = std::time(nullptr);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues) {
    // insert current value to the comparison window
    this->lastValuesWindow.emplace(measuredValues.value());

    // simplified prediction phase
    xHatNew = stateTransitionModel * xHat;// no control unit (B*u), predicted a-priori state estimate
    estimateCovariance = stateTransitionModel * estimateCovariance * stateTransitionModel.transpose()
        + processNoiseCovariance;// predicted a-priori estimate covariance

    /**
     * Simplified update phase, use the
     * measured values to upate the innovation
     * error, calculate Kalman gain kalmanGain (reward)
     * and update the posteriori state
     * estimate. The updated state estimate
     * becomes the new xHat (current state).
     */
    innovationError = measuredValues - (observationModel * xHatNew);// update innovation error ψ_k, eq. 2 + 3
    kalmanGain = estimateCovariance * observationModel.transpose()
        * (observationModel * estimateCovariance * observationModel.transpose() + measurementNoiseCovariance)
              .inverse();                                                   // kalman gain
    xHatNew += kalmanGain * (measuredValues - (observationModel * xHatNew));// updated a-posteriori state estimate
    estimateCovariance =
        (identityMatrix - kalmanGain * observationModel) * estimateCovariance;// updated a-posteriori estimate covariance
    xHat = xHatNew;                                                           // updated xHat

    // update estimation error, eq.8
    this->estimationError =
        std::sqrt(((innovationError * measuredValues.inverse()) * (innovationError * measuredValues.inverse())).trace());
    this->kfErrorWindow.emplace(this->estimationError);// store result in error window
    // update timestep
    currentTime += timeStep;
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep) {
    timeStep = newTimeStep;
    this->update(measuredValues);
}

void KalmanFilter::update(const Eigen::VectorXd& measuredValues, double newTimeStep, const Eigen::MatrixXd& F) {
    this->stateTransitionModel = F;
    timeStep = newTimeStep;
    this->update(measuredValues);
}

double KalmanFilter::getTotalEstimationError() { return this->calculateTotalEstimationError(); }

float KalmanFilter::calculateTotalEstimationError() {
    float j = 1;// eq. 9 iterator
    float totalError = 0;
    for (auto errorValue : kfErrorWindow) {
        totalError += (errorValue / j);
        ++j;
    }
    return totalError / totalEstimationErrorDivider;
}

void KalmanFilter::calculateTotalEstimationErrorDivider(int size) {
    totalEstimationErrorDivider = size > 0 ? 0 : 1;
    for (int i = 1; i <= size; ++i) {
        totalEstimationErrorDivider += (1.0 / i);
    }
}

std::chrono::milliseconds KalmanFilter::getNewGatheringInterval() {
    // eq. 10
    auto totalEstimationError = this->calculateTotalEstimationError();
    auto powerOfEuler = (totalEstimationError + lambda) / lambda;
    auto thetaPart = theta * (1 - std::pow(eulerConstant, powerOfEuler));
    auto newGatheringIntervalCandidate = this->gatheringInterval.count() + thetaPart;
    if (newGatheringIntervalCandidate >= gatheringIntervalReceived.count() - (gatheringIntervalRange.count() / 2)
        && newGatheringIntervalCandidate <= gatheringIntervalReceived.count() + (gatheringIntervalRange.count() / 2)) {// eq. 7
        // remove fractional part from double
        this->gatheringInterval = std::chrono::milliseconds((int) trunc(newGatheringIntervalCandidate));
    }
    return this->gatheringInterval;
}

std::chrono::milliseconds KalmanFilter::getValueMagnitudeBasedFrequency() {
    auto totalEstimationError = this->calculateTotalEstimationError();
    auto powerOfEuler = (totalEstimationError + lambda) / lambda;
    auto thetaPart = theta * (1 - std::pow(eulerConstant, powerOfEuler));
    auto newFreqCandidate = this->gatheringInterval.count() + thetaPart;
    auto magnitude = this->getMagnitudeFromLastValues(this->lastValuesWindow[1], this->lastValuesWindow[0]);
    auto canonicalizedFreqCandidate = (magnitude > 0) ? newFreqCandidate / magnitude : newFreqCandidate;
    // forget upper/lower thresholds or error thresholds for now
    this->gatheringInterval = std::chrono::milliseconds((int) trunc(canonicalizedFreqCandidate));
    return this->gatheringInterval;
}

std::chrono::milliseconds KalmanFilter::getExponentialFrequency() {
    /**
     * diff is new - old, negative means new is small
     * so we don't need to cover it
     */
    if (this->getEstimationErrorDifference() > 0.6) {
        return this->getExponentialDecayFrequency();
    } else if (this->getTotalEstimationError() < 0.24) {
        return this->getExponentialGrowthFrequency();
    }
    return this->gatheringInterval;
}

double KalmanFilter::getEstimationErrorDifference() {
    return this->kfErrorWindow[0] - this->kfErrorWindow[1];
}

// TODO: remove the triggering if clause, we still use it for testing
std::chrono::milliseconds KalmanFilter::getExponentialDecayFrequency() {
    if (this->getEstimationErrorDifference() > 0.6) {
        auto newFreqCandidate = this->initialFreq.count() * std::pow((1 - .25), this->decreaseCounter); // y = y0 * ((1-b) ^ x)
        ++this->decreaseCounter;
        this->increaseCounter = 1;
        this->gatheringInterval = std::chrono::milliseconds((int) std::abs(trunc(newFreqCandidate)));
    }
    return this->gatheringInterval;
}

// TODO: remove the triggering if clause, we still use it for testing
std::chrono::milliseconds KalmanFilter::getExponentialGrowthFrequency() {
    if (this->getTotalEstimationError() < 0.24) {
        auto newFreqCandidate = this->initialFreq.count() * std::pow((1 + .25), this->increaseCounter); // y = y0 * ((1+b) ^ x)
        ++this->increaseCounter;
        this->decreaseCounter = 1;
        this->gatheringInterval = std::chrono::milliseconds((int) trunc(newFreqCandidate));
    }
    return this->gatheringInterval;
}

double KalmanFilter::getMagnitudeFromLastValues(double oldValue, double newValue) {
    auto oldValueMod = std::log10(std::abs(oldValue));
    auto newValueMod = std::log10(std::abs(newValue));
    auto suggestedMagnitudeMultiplier = std::abs(oldValueMod - newValueMod);
    return std::trunc(suggestedMagnitudeMultiplier);
}

void KalmanFilter::updateFromTupleBuffer(Runtime::TupleBuffer& tupleBuffer) {
    NES_DEBUG("KalmanFilter::updateFromTupleBuffer: updating from a whole tuple buffer");
    if (!!tupleBuffer) {
        Eigen::VectorXd valueVector(1);
        auto numOfTuples = tupleBuffer.getNumberOfTuples();
        auto records = tupleBuffer.getBuffer<Sensors::SingleSensor>();
        for (uint64_t i = 0; i < numOfTuples; ++i) {
            valueVector << records[i].value;
            this->update(valueVector);
        }
        NES_DEBUG("KalmanFilter::updateFromTupleBuffer: consumed a whole buffer");
    }
}

double KalmanFilter::getCurrentStep() { return currentTime; }
Eigen::VectorXd KalmanFilter::getState() { return xHat; }
Eigen::MatrixXd KalmanFilter::getError() { return estimateCovariance; }
Eigen::MatrixXd KalmanFilter::getInnovationError() { return innovationError; }
double KalmanFilter::getEstimationError() { return estimationError; }
uint64_t KalmanFilter::getTheta() { return theta; }
float KalmanFilter::getLambda() { return lambda; }

void KalmanFilter::setLambda(float newLambda) { this->lambda = newLambda; }


void KalmanFilter::setGatheringInterval(std::chrono::milliseconds gatheringIntervalInMillis) {
    this->gatheringInterval = gatheringIntervalInMillis;
    this->gatheringIntervalReceived = gatheringIntervalInMillis;
    initialFreq = gatheringIntervalInMillis;
}
void KalmanFilter::setGatheringIntervalRange(std::chrono::milliseconds gatheringIntervalRange) {
    this->gatheringIntervalRange = gatheringIntervalRange;
}
void KalmanFilter::setGatheringIntervalWithRange(std::chrono::milliseconds gatheringIntervalInMillis,
                                                 std::chrono::milliseconds gatheringIntervalRange) {
    this->setGatheringInterval(gatheringIntervalInMillis);
    this->setGatheringIntervalRange(gatheringIntervalRange);
}

std::chrono::milliseconds KalmanFilter::getCurrentGatheringInterval() { return this->gatheringInterval; }

}// namespace NES