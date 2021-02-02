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

#ifndef API_SCHEMA_H
#define API_SCHEMA_H

#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <memory>
#include <string>

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <vector>

namespace NES {
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class Schema {
  public:
    Schema();
    Schema(SchemaPtr query);

    /**
     * @brief Schema qualifier separator
     */
    constexpr static const char* const ATTRIBUTE_NAME_SEPARATOR = "$";

    /**
     * @brief Schema qualifier for field with undefined qualifier
     */
    constexpr static const char* const UNDEFINED_SCHEMA_QUALIFIER = "_$";

    /**
     * @brief Factory method to create a new SchemaPtr.
     * @return SchemaPtr
     */
    static SchemaPtr create();

    /**
     * @brief Creates a copy of this schema.
     * @note The containing AttributeFields may still reference the same objects.
     * @return A copy of the Schema
     */
    SchemaPtr copy() const;

    /**
     * @brief Copy all fields of otherSchema into this schema.
     * @param otherSchema
     * @return a copy of this schema.
     */
    SchemaPtr copyFields(SchemaPtr otherSchema);

    /**
     * @brief appends a AttributeField to the schema and returns a copy of this schema.
     * @param attribute
     * @return a copy of this schema.
     */
    SchemaPtr addField(AttributeFieldPtr attribute);

    /**
    * @brief appends a field with a basic type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, const BasicType& type);

    /**
    * @brief appends a field with a data type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, DataTypePtr data);

    /**
     * @brief removes a AttributeField from the schema
     * @param field
     */
    void removeField(AttributeFieldPtr field);

    /**
     * @brief Replaces a field, which is already part of the schema.
     * @param name
     * @param type
     * @return
     */
    void replaceField(const std::string& name, DataTypePtr type);

    /**
     * @brief Checks if an attribute with the input field name is defined in the schema
     * @param fieldName: fully or partly qualified field name
     * @return AttributeFieldPtr: pointer to attribute field if present else null pointer
     */
    AttributeFieldPtr hasFieldName(const std::string& fieldName);

    /**
     * @brief Checks if attribute field name is defined in the schema and returns its index.
     * If item not in the list, then the return value is equal to fields.size().
     * @param fieldName
     * @return the index
     */
    uint64_t getIndex(const std::string& fieldName);

    /**
     * @brief Finds a attribute field by name in the schema
     * @param fieldName
     * @return AttributeField
     */
    AttributeFieldPtr get(const std::string& fieldName);

    /**
     * @brief Finds a attribute field by index in the schema
     * @param index
     * @return AttributeField
     */
    AttributeFieldPtr get(uint32_t index);

    /**
     * @brief Returns the number of fields in the schema.
     * @return uint64_t
     */
    uint64_t getSize() const;

    /**
     * @brief Returns the number of bytes all fields in this schema occupy.
     * @return uint64_t
     */
    uint64_t getSchemaSizeInBytes() const;

    /**
     * @brief Checks if two Schemas are equal to each other.
     * @param schema
     * @param considerOrder takes into account if the order of fields in a schema matter.
     * @return boolean
     */
    bool equals(SchemaPtr schema, bool considerOrder = true);

    /**
     * @brief Checks if two schemas have same datatypes at same index location
     * @param otherSchema: the other schema to compare agains
     * @return ture if they are equal else false
     */
    bool hasEqualTypes(SchemaPtr otherSchema);

    /**
     * @brief Checks if the field exists in the schema
     * @param schema
     * @return boolean
    */
    bool contains(const std::string& fieldName);

    const std::string toString() const;

    /**
     * @brief Remove all fields and qualifying name
     */
    void clear();

    std::vector<AttributeFieldPtr> fields;
};

AttributeFieldPtr createField(std::string name, BasicType type);

}// namespace NES
#endif// API_SCHEMA_H
