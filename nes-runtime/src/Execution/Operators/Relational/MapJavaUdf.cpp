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
#ifdef ENABLE_JNI

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JVMContext.hpp>
#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/MapJavaUdfOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstring>
#include <filesystem>
#include <jni.h>
#include <utility>
#if not(defined(__APPLE__))
#include <experimental/source_location>
#endif

namespace NES::Runtime::Execution::Operators {

/**
 * This function is used for JNI error handling.
 * @param env jni environment
 * @param location location of the error. Leave default to use the location of the caller.
 */
inline void jniErrorCheck(JNIEnv* env, const std::source_location& location = std::source_location::current()) {
    auto exception = env->ExceptionOccurred();
    if (exception) {
        // print exception
        jboolean isCopy = false;
        auto clazz = env->FindClass("java/lang/Object");
        auto toString = env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
        auto string = (jstring) env->CallObjectMethod(exception, toString);
        const char* utf = env->GetStringUTFChars(string, &isCopy);
        NES_THROW_RUNTIME_ERROR("An error occurred during a map java UDF execution in function "
                                << location.function_name() << " at line " << location.line() << ": " << utf);
    }
}

/**
 * Returns if directory of path exists.
 * @param name path to check
 * @return bool if directory exists
 */
inline bool dirExists(const std::string& path) { return std::filesystem::exists(path.c_str()); }

/**
 * loads clases from the byteCodeList into the JVM
 * @param state operator handler state
 * @param byteCodeList byte code list
 */
extern "C" void loadClassesFromByteList(void* state, const std::unordered_map<std::string, std::vector<char>>& byteCodeList) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    for (auto entry : byteCodeList) {
        auto bufLen = entry.second.size();
        const auto byteCode = reinterpret_cast<jbyte*>(entry.second.data());
        handler->getEnvironment()->DefineClass(entry.first.c_str(), nullptr, byteCode, (jsize) bufLen);
    }
}

/**
 * Deserializes the given instance
 * @param state operator handler state
 */
extern "C" jobject deserializeInstance(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    // use deserializer given in java utils file
    void* object = (void*) handler->getSerializedInstance().data();
    auto clazz = handler->getEnvironment()->FindClass("MapJavaUdfUtils");
    jniErrorCheck(handler->getEnvironment());
    // TODO: we can probably cache the method id for all functions in e.g. the operator handler to improve performance
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "deserialize", "(Ljava/nio/ByteBuffer;)Ljava/lang/Object;");
    jniErrorCheck(handler->getEnvironment());
    auto obj = handler->getEnvironment()->CallStaticObjectMethod(clazz, mid, object);
    jniErrorCheck(handler->getEnvironment());
    return obj;
}

/**
 * Start the java vm and load the classes given in the javaPath
 * @param state operator handler state
 */
extern "C" void startOrAttachVMWithJarFile(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    // Sanity check javaPath
    auto javaPath = handler->getJavaPath().value();
    if (!dirExists(javaPath)) {
        NES_FATAL_ERROR("jarPath:" << javaPath << " not valid!");
        exit(EXIT_FAILURE);
    }

    JavaVMInitArgs vmArgs;
    auto* options = new JavaVMOption[3];
    std::string classPathOpt = std::string("-Djava.class.path=") + javaPath;
    options[0].optionString = (char*) classPathOpt.c_str();
    options[1].optionString = (char*) "-verbose:jni";
    options[2].optionString = (char*) "-verbose:class";
    vmArgs.nOptions = 3;
    vmArgs.options = options;
    vmArgs.version = JNI_VERSION_1_2;
    vmArgs.ignoreUnrecognized = false;// invalid options make the JVM init fail

    auto env = handler->getEnvironment();
    JVMContext::instance().createOrAttachToJVM(&env, vmArgs);
    handler->setEnvironment(env);
}

/**
 * Start the java vm and load the classes given in the byteCodeList
 * @param state operator handler state
 */
extern "C" void startOrAttachVMWithByteList(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    JavaVMInitArgs vmArgs;
    vmArgs.version = JNI_VERSION_1_2;
    vmArgs.ignoreUnrecognized = false;// invalid options make the JVM init fail

    JVMContext& context = JVMContext::instance();
    auto env = handler->getEnvironment();
    context.createOrAttachToJVM(&env, vmArgs);
    handler->setEnvironment(env);
    loadClassesFromByteList(handler, handler->getByteCodeList());
}

/**
 * Wrapper for starting or attaching to the java vm.
 * The java classes will be either loaded from the given jar file or from the given byte code list.
 * When no java path is given, the byte code list is used.
 * @param state operator handler state
 */
extern "C" void startOrAttachVM(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    if (handler->getJavaPath().has_value()) {
        // Get java classes using jar files
        startOrAttachVMWithJarFile(state);
    } else {
        // Get java classes using byte list
        startOrAttachVMWithByteList(state);
    }
}

/**
 * Detach the current thread from the JVM.
 * This is needed to avoid memory leaks.
 */
extern "C" void detachVM() { JVMContext::instance().detachFromJVM(); }

/**
 * Unloads the java VM.
 * This is needed to avoid memory leaks.
 */
extern "C" void destroyVM() { JVMContext::instance().destroyJVM(); }

/**
 * Finds the input class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @return jclass input class object pointer
 */
extern "C" void* findInputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getInputClassName().c_str());
    jniErrorCheck(handler->getEnvironment());
    return clazz;
}

/**
 * Finds the output class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @class jclass output class object pointer
 */
extern "C" void* findOutputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getOutputClassName().c_str());
    jniErrorCheck(handler->getEnvironment());
    return clazz;
}

/**
 * Allocates a new instance of the given class.
 * @param state operator handler state
 * @param classPtr class to allocate
 * @return jobject instance of the given class
 */
extern "C" void* allocateObject(void* state, void* classPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = (jclass) classPtr;
    jobject obj = handler->getEnvironment()->AllocObject(clazz);
    jniErrorCheck(handler->getEnvironment());
    return obj;
}

/**
 * Creates a new instance of a class and sets its value in the constructor.
 * @tparam T type of the class
 * @param state operator handler state
 * @param value value to set
 * @param className name of the class
 * @param constructorSignature signature of the constructor
 * @return jobject instance of the given class
 */
template<typename T>
void* createObjectType(void* state, T value, std::string className, std::string constructorSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "<init>", constructorSignature.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto object = handler->getEnvironment()->NewObject(clazz, mid, value);
    jniErrorCheck(handler->getEnvironment());
    return object;
}

/**
 * Creates a new boolean object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createBooleanObject(void* state, bool value) {
    return createObjectType(state, value, "java/lang/Boolean", "(Z)V");
}

/**
 * Creates a new float object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createFloatObject(void* state, float value) { return createObjectType(state, value, "java/lang/Float", "(F)V"); }

/**
 * Creates a new double object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createDoubleObject(void* state, double value) {
    return createObjectType(state, value, "java/lang/Double", "(D)V");
}

/**
 * Creates a new int object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createIntegerObject(void* state, int32_t value) {
    return createObjectType(state, value, "java/lang/Integer", "(I)V");
}

/**
 * Creates a new long object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createLongObject(void* state, int64_t value) { return createObjectType(state, value, "java/lang/Long", "(J)V"); }

/**
 * Creates a new short object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createShortObject(void* state, int16_t value) {
    return createObjectType(state, value, "java/lang/Short", "(S)V");
}

/**
 * Creates a new java byte object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createByteObject(void* state, int8_t value) { return createObjectType(state, value, "java/lang/Byte", "(B)V"); }

/**
 * Creates a new string object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
extern "C" void* createStringObject(void* state, TextValue* value) {
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);
    return handler->getEnvironment()->NewStringUTF(value->c_str());
}

/**
 * Get the value of an object of type boolean, float, double, int, long, short, byte or string.
 * @tparam T type of value
 * @param state operator handler state
 * @param object object to get the value from
 * @param className class name of the object
 * @param getterName getter function name of the value
 * @param getterSignature getter function signature of the value
 * @return T value of the field
 */
template<typename T>
T getObjectTypeValue(void* state, void* object, std::string className, std::string getterName, std::string getterSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto mid = handler->getEnvironment()->GetMethodID(clazz, getterName.c_str(), getterSignature.c_str());
    jniErrorCheck(handler->getEnvironment());
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = handler->getEnvironment()->CallBooleanMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, float>::value) {
        value = handler->getEnvironment()->CallFloatMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, double>::value) {
        value = handler->getEnvironment()->CallDoubleMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = handler->getEnvironment()->CallIntMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = handler->getEnvironment()->CallLongMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = handler->getEnvironment()->CallShortMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = handler->getEnvironment()->CallByteMethod((jobject) object, mid);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment());
    return value;
}

/**
 * Get boolean value of a bool object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return bool value of the field
 */
extern "C" bool getBooleanObjectValue(void* state, void* object) {
    return getObjectTypeValue<bool>(state, object, "java/lang/Boolean", "booleanValue", "()Z");
}

/**
 * Get float value of a flaot object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return float value of the field
 */
extern "C" float getFloatObjectValue(void* state, void* object) {
    return getObjectTypeValue<float>(state, object, "java/lang/Float", "floatValue", "()F");
}

/**
 * Get double value of a double object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return double value of the field
 */
extern "C" double getDoubleObjectValue(void* state, void* object) {
    return getObjectTypeValue<double>(state, object, "java/lang/Double", "doubleValue", "()D");
}

/**
 * Get int value of a integer object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return int value of the field
 */
extern "C" int32_t getIntegerObjectValue(void* state, void* object) {
    return getObjectTypeValue<int32_t>(state, object, "java/lang/Integer", "intValue", "()I");
}

/**
 * Get long value of a long object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return long value of the field
 */
extern "C" int64_t getLongObjectValue(void* state, void* object) {
    return getObjectTypeValue<int64_t>(state, object, "java/lang/Long", "longValue", "()J");
}

/**
 * Get short value of a short object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return short value of the field
 */
extern "C" int16_t getShortObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Short", "shortValue", "()S");
}

/**
 * Get byte value of
 * @param state operator handler state
 * @param object object to get the field from
 * @return byte value of the field
 */
extern "C" int8_t getByteObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Byte", "byteValue", "()B");
}

/**
 * Get string value of a string object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return TextValue value of the field
 */
extern "C" TextValue* getStringObjectValue(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(object != nullptr, "object should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto size = handler->getEnvironment()->GetStringUTFLength((jstring) object);
    auto resultText = TextValue::create(size);
    auto sourceText = handler->getEnvironment()->GetStringUTFChars((jstring) object, nullptr);
    std::memcpy(resultText->str(), sourceText, size);
    return resultText;
}

/**
 * Get the value of a field of an object.
 * @tparam T type of the field
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @param signature signature of the field
 * @return T value of the field
 */
template<typename T>
T getField(void* state, void* classPtr, void* objectPtr, int fieldIndex, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment());
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = (T) handler->getEnvironment()->GetBooleanField(pojo, id);
    } else if constexpr (std::is_same<T, float>::value) {
        value = (T) handler->getEnvironment()->GetFloatField(pojo, id);
    } else if constexpr (std::is_same<T, double>::value) {
        value = (T) handler->getEnvironment()->GetDoubleField(pojo, id);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = (T) handler->getEnvironment()->GetIntField(pojo, id);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = (T) handler->getEnvironment()->GetLongField(pojo, id);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = (T) handler->getEnvironment()->GetShortField(pojo, id);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = (T) handler->getEnvironment()->GetByteField(pojo, id);
    } else if constexpr (std::is_same<T, TextValue*>::value) {
        auto jstr = (jstring) handler->getEnvironment()->GetObjectField(pojo, id);
        auto size = handler->getEnvironment()->GetStringUTFLength((jstring) jstr);
        value = TextValue::create(size);
        auto resultText = handler->getEnvironment()->GetStringUTFChars(jstr, 0);
        std::memcpy(value->str(), resultText, size);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment());
    return value;
}

/**
 * Get the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return bool value of the field
 */
extern "C" bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<bool>(state, classPtr, objectPtr, fieldIndex, "Z");
}

/**
 * Get the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return float value of the field
 */
extern "C" float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<float>(state, classPtr, objectPtr, fieldIndex, "F");
}

/**
 * Get the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return double value of the field
 */
extern "C" double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<double>(state, classPtr, objectPtr, fieldIndex, "D");
}

/**
 * Get the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int32_t value of the field
 */
extern "C" int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int32_t>(state, classPtr, objectPtr, fieldIndex, "I");
}

/**
 * Get the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int64_t value of the field
 */
extern "C" int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int64_t>(state, classPtr, objectPtr, fieldIndex, "J");
}

/**
 * Get the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int16_t value of the field
 */
extern "C" int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int16_t>(state, classPtr, objectPtr, fieldIndex, "S");
}

/**
 * Get the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int8_t value of the field
 */
extern "C" int8_t getByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int8_t>(state, classPtr, objectPtr, fieldIndex, "B");
}

/**
 * Get the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return TextValue* value of the field
 */
extern "C" TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<TextValue*>(state, classPtr, objectPtr, fieldIndex, "Ljava/lang/String;");
}

/**
 * Set the value of a field of an object.
 * @tparam T type of the field
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 * @param signature signature of the field
 */
template<typename T>
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment());
    if constexpr (std::is_same<T, bool>::value) {
        handler->getEnvironment()->SetBooleanField(pojo, id, (jboolean) value);
    } else if constexpr (std::is_same<T, float>::value) {
        handler->getEnvironment()->SetFloatField(pojo, id, (jfloat) value);
    } else if constexpr (std::is_same<T, double>::value) {
        handler->getEnvironment()->SetDoubleField(pojo, id, (jdouble) value);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        handler->getEnvironment()->SetIntField(pojo, id, (jint) value);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        handler->getEnvironment()->SetLongField(pojo, id, (jlong) value);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        handler->getEnvironment()->SetShortField(pojo, id, (jshort) value);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        handler->getEnvironment()->SetByteField(pojo, id, (jbyte) value);
    } else if constexpr (std::is_same<T, const TextValue*>::value) {
        const TextValue* sourceString = value;
        jstring string = handler->getEnvironment()->NewStringUTF(sourceString->c_str());
        handler->getEnvironment()->SetObjectField(pojo, id, (jstring) string);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment());
}

/**
 * Set the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Z");
}

/**
 * Set the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "F");
}

/**
 * Set the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "D");
}

/**
 * Set the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "I");
}

/**
 * Set the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "J");
}

/**
 * Set the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "S");
}

/**
 * Set the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int8_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "B");
}

/**
 * Set the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
extern "C" void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const TextValue* value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Ljava/lang/String;");
}

/**
 * free a jvm object
 * @param state operator handler state
 * @param object object to free
 */
extern "C" void freeObject(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    handler->getEnvironment()->DeleteLocalRef((jobject) object);
}

/**
 * Execute the java udf
 * @param state operator handler state
 * @param pojoObjectPtr pojo object
 * @return result of the udf
 */
extern "C" void* executeUdf(void* state, void* pojoObjectPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(pojoObjectPtr != nullptr, "pojoObjectPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    // Find class implementing the map udf
    jclass c1 = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
    jniErrorCheck(handler->getEnvironment());

    // Build function signature of map function
    std::string sig = "(L" + handler->getInputClassName() + ";)L" + handler->getOutputClassName() + ";";

    // Find udf function
    jmethodID mid = handler->getEnvironment()->GetMethodID(c1, handler->getMethodName().c_str(), sig.c_str());
    jniErrorCheck(handler->getEnvironment());

    jobject udf_result, instance;
    // The map udf class will be either loaded from a serialized instance or allocated using class information
    if (!handler->getSerializedInstance().empty()) {
        // Load instance if defined
        instance = deserializeInstance(state);
    } else {
        // Create instance object using class information
        jclass clazz = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
        jniErrorCheck(handler->getEnvironment());

        // Here we assume the default constructor is available
        auto constr = handler->getEnvironment()->GetMethodID(clazz, "<init>", "()V");
        instance = handler->getEnvironment()->NewObject(clazz, constr);
        jniErrorCheck(handler->getEnvironment());
    }

    // Call udf function
    udf_result = handler->getEnvironment()->CallObjectMethod(instance, mid, pojoObjectPtr);
    jniErrorCheck(handler->getEnvironment());
    return udf_result;
}

/**
 * Operator execution function
 * @param ctx operator context
 * @param record input record
 */
void MapJavaUdf::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // We need to ensure that the current thread is attached to the JVM
    // When we can guarantee that the operator is always executed in the same thread we can remove attaching and detaching
    FunctionCall("startOrAttachVM", startOrAttachVM, handler);

    // Allocate java input class
    auto inputClassPtr = FunctionCall("findInputClass", findInputClass, handler);
    auto inputPojoPtr = FunctionCall("allocateObject", allocateObject, handler, inputClassPtr);

    // Loading record values into java input class
    // We derive the types of the values from the schema. The type can be complex of simple.
    // 1. Simple: tuples with one field represented through an object type (String, Integer, ..)
    // 2. Complex: plain old java object containing the multiple primitive types
    if (inputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        auto field = inputSchema->fields[0];
        auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            inputPojoPtr =
                FunctionCall<>("createBooleanObject", createBooleanObject, handler, record.read(fieldName).as<Boolean>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            inputPojoPtr = FunctionCall<>("createFloatObject", createFloatObject, handler, record.read(fieldName).as<Float>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            inputPojoPtr = FunctionCall<>("createDoubleObject", createDoubleObject, handler, record.read(fieldName).as<Double>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            inputPojoPtr =
                FunctionCall<>("createIntegerObject", createIntegerObject, handler, record.read(fieldName).as<Int32>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            inputPojoPtr = FunctionCall<>("createLongObject", createLongObject, handler, record.read(fieldName).as<Int64>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            inputPojoPtr = FunctionCall<>("createShortObject", createShortObject, handler, record.read(fieldName).as<Int16>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            inputPojoPtr = FunctionCall<>("createByteObject", createByteObject, handler, record.read(fieldName).as<Int8>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            inputPojoPtr = FunctionCall<>("createStringObject",
                                          createStringObject,
                                          handler,
                                          record.read(fieldName).as<Text>()->getReference());
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
            auto field = inputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                FunctionCall<>("setBooleanField",
                               setBooleanField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Boolean>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                FunctionCall<>("setFloatField",
                               setFloatField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                FunctionCall<>("setDoubleField",
                               setDoubleField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Double>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                FunctionCall<>("setIntegerField",
                               setIntegerField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int32>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                FunctionCall<>("setLongField",
                               setLongField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int64>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                FunctionCall<>("setShortField",
                               setShortField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int16>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                FunctionCall<>("setByteField",
                               setByteField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int8>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                FunctionCall<>("setStringField",
                               setStringField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Text>()->getReference());
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
    }

    // Get output class and call udf
    auto outputClassPtr = FunctionCall<>("findOutputClass", findOutputClass, handler);
    auto outputPojoPtr = FunctionCall<>("executeUdf", executeUdf, handler, inputPojoPtr);

    FunctionCall<>("freeObject", freeObject, handler, inputPojoPtr);

    // Create new record for result
    record = Record();

    // Reading result values from jvm into result record
    // Same differentiation as for input class above
    if (outputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        auto field = outputSchema->fields[0];
        auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            Value<> val = FunctionCall<>("getBooleanObjectValue", getBooleanObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            Value<> val = FunctionCall<>("getFloatObjectValue", getFloatObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            Value<> val = FunctionCall<>("getDoubleObjectValue", getDoubleObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            Value<> val = FunctionCall<>("getIntegerObjectValue", getIntegerObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            Value<> val = FunctionCall<>("getLongObjectValue", getLongObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            Value<> val = FunctionCall<>("getShortObjectValue", getShortObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            Value<> val = FunctionCall<>("getByteObjectValue", getByteObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            Value<> val = FunctionCall<>("getStringObjectValue", getStringObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) outputSchema->fields.size(); i++) {
            auto field = outputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                Value<> val =
                    FunctionCall<>("getBooleanField", getBooleanField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                Value<> val =
                    FunctionCall<>("getFloatField", getFloatField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                Value<> val =
                    FunctionCall<>("getDoubleField", getDoubleField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                Value<> val =
                    FunctionCall<>("getIntegerField", getIntegerField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                Value<> val =
                    FunctionCall<>("getLongField", getLongField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                Value<> val =
                    FunctionCall<>("getShortField", getShortField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                Value<> val =
                    FunctionCall<>("getByteField", getByteField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                Value<> val =
                    FunctionCall<>("getStringField", getStringField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
    }

    FunctionCall<>("freeObject", freeObject, handler, outputPojoPtr);
    FunctionCall<>("detachJVM", detachVM);

    // Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

/**
 * Terminate operator
 * @param ctx execution context
 */
void MapJavaUdf::terminate(ExecutionContext&) const {
    // TODO fix usage of jvm
    //FunctionCall<>("destroyVM", destroyVM);
}

}// namespace NES::Runtime::Execution::Operators
#endif// ENABLE_JIN