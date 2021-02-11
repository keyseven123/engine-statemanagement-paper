/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_CONFIGOPTION_HPP
#define NES_CONFIGOPTION_HPP

#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <any>
#include <sstream>
#include <string>
#include <typeinfo>

namespace NES {

/**
 * @brief Template for a ConfigOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ConfigOption {
  public:
    static std::shared_ptr<ConfigOption> create(std::string name, T value, std::string description) {
        return std::make_shared<ConfigOption>(ConfigOption(name, value, description));
    };

    /**
     * @brief converts a ConfigOption Object into human readable format
     * @return string representation of the config
     */
    std::string toString() {
        std::stringstream ss;
        ss << "Config Object: \n";
        ss << "Name: " << name << "\n";
        ss << "Description: " << description << "\n";
        ss << "Value: " << value << "\n";
        ss << "Default Value: " << defaultValue << "\n";
        return ss.str();
    }

    /**
     * @brief converts the value of this object into a string
     * @return string of the value of this object
     */
    std::string getValueAsString() const { return string(value); };

    /**
     * @brief get the name of the ConfigOption Object
     * @return name of the config
     */
    std::string getName() { return name; }

    /**
     * @brief get the value of the ConfigOption Object
     * @return the value of the config if not set then default value
     */
    T getValue() const { return value; };

    /**
     * @brief sets the value
     * @param value: the value to be used
     */
    void setValue(T value) { this->value = value; }

    /**
     * @brief get the description of this parameter
     * @return description of the config
     */
    const std::string getDescription() const { return description; };

    /**
     * @brief get the default value of this parameter
     * @return: the default value
     */
    T getDefaultValue() const { return defaultValue; };

    /**
     * @brief perform equality check between two config options
     * @param other: other config option
     * @return true if equal else return false
     */
    bool equals(std::any other) {
        if (this == other) {
            return true;
        } else if (other.has_value() && other.type() == typeid(ConfigOption)) {
            ConfigOption<T> that = (ConfigOption<T>) other;
            return this->name == that.name && this->description == that.description && this->value == that.value
                && this->defaultValue == that.defaultValue;
        }
        return false;
    };

  private:
    /**
     * @brief Constructs a ConfigOption<T> object
     * @param name the name of the object
     * @param value the value of the object
     * @param description default value of the object
     */
    ConfigOption(std::string name, T value, std::string description)
        : name(name), description(description), value(value), defaultValue(value) {}

    std::string name;
    std::string description;
    T value;
    T defaultValue;
};
}// namespace NES

#endif//NES_CONFIGOPTION_HPP
