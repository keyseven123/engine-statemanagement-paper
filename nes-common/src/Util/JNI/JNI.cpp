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

#include <Util/JNI/JNI.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <jni.h>
#include <string>
#include <string_view>

namespace jni {

// Static Variables
static std::atomic_bool isJVMInitialized(false);
static JavaVM* jvmInstance = nullptr;

static bool getEnv(JavaVM* vm, JNIEnv** env) { return vm->GetEnv((void**) env, JNI_VERSION_1_8) == JNI_OK; }

static bool isAttached(JavaVM* vm) {
    JNIEnv* env = nullptr;
    return getEnv(vm, &env);
}

/**
* Maintains the lifecycle of a JNIEnv.
*/
class ScopedEnv final {
  public:
    ScopedEnv() noexcept : vm(nullptr), env(nullptr), attached(false) {}
    ~ScopedEnv();

    void init(JavaVM* vm);
    JNIEnv* get() const noexcept { return env; }

  private:
    // Instance Variables
    JavaVM* vm;
    JNIEnv* env;
    bool attached;///< Manually attached, as opposed to already attached.
};

ScopedEnv::~ScopedEnv() {
    if (vm && attached) {
        vm->DetachCurrentThread();
    }
}

void ScopedEnv::init(JavaVM* vm) {
    if (env != nullptr) {
        return;
    }

    if (vm == nullptr) {
        throw InitializationException("JNI not initialized");
    }
    if (!getEnv(vm, &env)) {
        if (vm->AttachCurrentThread((void**) &env, nullptr) != 0) {
            throw InitializationException("Could not attach JNI to thread");
        }
        attached = true;
    }

    this->vm = vm;
}

JNIEnv* getEnv() {
    static thread_local ScopedEnv env;

    if (env.get() != nullptr && !isAttached(jvmInstance)) {
        // we got detached, so clear it.
        // will be re-populated from static javaVm below.
        env = ScopedEnv{};
    }

    if (env.get() == nullptr) {
        env.init(jvmInstance);
    }

    return env.get();
}

void detatchEnv() {
    if (jvmInstance == nullptr) {
        throw InitializationException("JNI not initialized");
    }
    jvmInstance->DetachCurrentThread();
}

JVM& JVM::get() {
    static JVM jvm = {};
    return jvm;
}

JVM::JVM() : options() {}

void JVM::init() {
    bool expected = false;
    if (!isJVMInitialized.compare_exchange_strong(expected, true)) {
        throw InitializationException("Java Virtual Machine already initialized");
    }

    if (jvmInstance == nullptr) {
        JavaVMInitArgs args{};
        std::vector<JavaVMOption> jvmOptions;
        for (const auto& s : options) {
            jvmOptions.push_back(JavaVMOption{.optionString = const_cast<char*>(s.c_str())});
        }
        auto classPathString = "-Djava.class.path=" + classPath;
        jvmOptions.push_back(JavaVMOption{.optionString = const_cast<char*>(classPathString.c_str())});
        args.version = JNI_VERSION_1_2;
        args.ignoreUnrecognized = false;
        args.options = jvmOptions.data();
        args.nOptions = std::size(jvmOptions);

        JNIEnv* env;
        if (JNI_CreateJavaVM(&jvmInstance, (void**) &env, &args) != 0) {
            isJVMInitialized.store(false);
            throw InitializationException("Java Virtual Machine failed during creation");
        }
    }
}

JVM& JVM::addClasspath(const std::string& path) {
    if (isInitialized()) {
        throw InitializationException(
            "ClassPath can't be changed to running jvm. All classpaths have been provided before JVM init is called.");
    }
    this->classPath = this->classPath + ":" + path;
    return *this;
}

JVM& JVM::addOption(const std::string& option) {
    if (isInitialized()) {
        throw InitializationException(
            "Options can't be attached to running jvm. All options have been provided before JVM init is called.");
    }
    options.emplace_back(option);
    return *this;
}
bool JVM::isInitialized() { return isJVMInitialized; }

}// namespace jni