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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

jobject deserializeInstanceFromData(const char* object) {
    auto clazz = jni::findClass("MapJavaUdfUtils");
    // TODO: we can probably cache the method id for all functions in e.g. the operator handler to improve performance
    auto mid = jni::getEnv()->GetMethodID(clazz, "deserialize", "(Ljava/nio/ByteBuffer;)Ljava/lang/Object;");
    jni::jniErrorCheck();
    auto obj = jni::getEnv()->CallStaticObjectMethod(clazz, mid, object);
    jni::jniErrorCheck();
    return obj;
}

jobject deserializeInstance(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    // use deserializer given in java utils file
    return deserializeInstanceFromData(handler->getSerializedInstance().data());
}

void freeObject(void* object) { jni::freeObject((jobject) object); }

void startOrAttachVMWithByteList(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
}

void* findInputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    return jni::findClass(handler->getInputClassJNIName());
}

void* allocateObject(void* clazzPtr) {
    NES_ASSERT2_FMT(clazzPtr != nullptr, "clazzPtr should not be null");
    return jni::allocateObject((jclass) clazzPtr);
}

void* findOutputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    auto clazz = jni::findClass(handler->getOutputClassJNIName());
    return jni::getEnv()->NewGlobalRef(clazz);
}

void* getObjectClass(void* object) {
    auto clazz = jni::getEnv()->GetObjectClass((jni::jobject) object);
    return jni::getEnv()->NewGlobalRef(clazz);
}

void* createBooleanObject(bool value) { return jni::createBoolean(value); }

void* createFloatObject(float value) { return jni::createFloat(value); }

void* createDoubleObject(double value) { return jni::createDouble(value); }

void* createIntegerObject(int32_t value) { return jni::createInteger(value); }

void* createLongObject(int64_t value) { return jni::createLong(value); }

void* createShortObject(int16_t value) { return jni::createShort(value); }

void* createByteObject(int8_t value) { return jni::createByte(value); }

void* createStringObject(TextValue* value) { return jni::createString(value->c_str()); }

bool getBooleanObjectValue(void* object) { return jni::getBooleanValue((jobject) object); }

float getFloatObjectValue(void* object) { return jni::getFloatValue((jobject) object); }

double getDoubleObjectValue(void* object) { return jni::getDoubleValue((jobject) object); }

int32_t getIntegerObjectValue(void* object) { return jni::getIntegerValue((jobject) object); }

int64_t getLongObjectValue(void* object) { return jni::getLongValue((jobject) object); }

int16_t getShortObjectValue(void* object) { return jni::getShortValue((jobject) object); }

int8_t getByteObjectValue(void* object) { return jni::getByteValue((jobject) object); }

TextValue* getStringObjectValue(void* object) {
    NES_ASSERT2_FMT(object != nullptr, "object should not be null");
    auto string = jni::getStringObjectValue((jstring) object);
    auto resultText = TextValue::create(string);
    return resultText;
}

template<typename T>
T getField(void* state, void* classPtr, void* objectPtr, int fieldIndex, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getUdfOutputSchema()->fields[fieldIndex]->getName();
    jfieldID id = jni::getEnv()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jni::jniErrorCheck();
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = (T) jni::getEnv()->GetBooleanField(pojo, id);
    } else if constexpr (std::is_same<T, float>::value) {
        value = (T) jni::getEnv()->GetFloatField(pojo, id);
    } else if constexpr (std::is_same<T, double>::value) {
        value = (T) jni::getEnv()->GetDoubleField(pojo, id);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = (T) jni::getEnv()->GetIntField(pojo, id);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = (T) jni::getEnv()->GetLongField(pojo, id);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = (T) jni::getEnv()->GetShortField(pojo, id);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = (T) jni::getEnv()->GetByteField(pojo, id);
    } else if constexpr (std::is_same<T, TextValue*>::value) {
        auto jstr = (jstring) jni::getEnv()->GetObjectField(pojo, id);
        auto size = jni::getEnv()->GetStringUTFLength((jstring) jstr);
        value = TextValue::create(size);
        auto resultText = jni::getEnv()->GetStringUTFChars(jstr, 0);
        std::memcpy(value->str(), resultText, size);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jni::jniErrorCheck();
    return value;
}

bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<bool>(state, classPtr, objectPtr, fieldIndex, "Z");
}

float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<float>(state, classPtr, objectPtr, fieldIndex, "F");
}

double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<double>(state, classPtr, objectPtr, fieldIndex, "D");
}

int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int32_t>(state, classPtr, objectPtr, fieldIndex, "I");
}

int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int64_t>(state, classPtr, objectPtr, fieldIndex, "J");
}

int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int16_t>(state, classPtr, objectPtr, fieldIndex, "S");
}

int8_t getByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int8_t>(state, classPtr, objectPtr, fieldIndex, "B");
}

TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<TextValue*>(state, classPtr, objectPtr, fieldIndex, "Ljava/lang/String;");
}

template<typename T>
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getUdfInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = jni::getEnv()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jni::jniErrorCheck();
    if constexpr (std::is_same<T, bool>::value) {
        jni::getEnv()->SetBooleanField(pojo, id, (jboolean) value);
    } else if constexpr (std::is_same<T, float>::value) {
        jni::getEnv()->SetFloatField(pojo, id, (jfloat) value);
    } else if constexpr (std::is_same<T, double>::value) {
        jni::getEnv()->SetDoubleField(pojo, id, (jdouble) value);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        jni::getEnv()->SetIntField(pojo, id, (jint) value);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        jni::getEnv()->SetLongField(pojo, id, (jlong) value);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        jni::getEnv()->SetShortField(pojo, id, (jshort) value);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        jni::getEnv()->SetByteField(pojo, id, (jbyte) value);
    } else if constexpr (std::is_same<T, const TextValue*>::value) {
        const TextValue* sourceString = value;
        jstring string = jni::getEnv()->NewStringUTF(sourceString->c_str());
        jni::getEnv()->SetObjectField(pojo, id, (jstring) string);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jni::jniErrorCheck();
}

void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Z");
}

void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "F");
}

void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "D");
}

void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "I");
}

void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "J");
}

void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "S");
}

void setByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int8_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "B");
}

void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const TextValue* value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Ljava/lang/String;");
}

}// namespace NES::Runtime::Execution::Operators