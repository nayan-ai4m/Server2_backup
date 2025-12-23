/** \example user_buffer.cpp
    This example describes the use of buffers allocated by the user or other frameworks.
    The given source code applies to handle one camera and image acquisition.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

#include <iostream>
#include <vector>
#include "neoapi/neoapi.hpp"

class TestBuffer : public NeoAPI::BufferBase {
 public:
    explicit TestBuffer(size_t size) {
        size_ = size;
        mem_ = new uint8_t[size_];
        RegisterMemory(mem_, size_);
    }
    ~TestBuffer() {
        UnregisterMemory();
        delete[] mem_;
    }
    size_t GetSize() {
        return size_;
    }
 private:
    size_t size_{0};
    uint8_t* mem_{nullptr};
};

int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
        camera.Connect();
        size_t payloadsize = static_cast<size_t>(camera.f().PayloadSize.Get());
        std::vector<TestBuffer*> buffers;
        while (buffers.size() < 5) {
            TestBuffer *buffer = new TestBuffer(payloadsize);
            camera.AddUserBuffer(buffer);
            buffers.push_back(buffer);
        }
        camera.f().ExposureTime.Set(10000);

        camera.SetUserBufferMode();
        NeoAPI::Image image = camera.GetImage();
        image.Save("user_buffer.bmp");
        image.GetUserBuffer<TestBuffer*>()->GetSize();

        while (buffers.size()) {
            TestBuffer *buffer = buffers.back();
            camera.RevokeUserBuffer(buffer);
            buffers.pop_back();
            delete buffer;
        }
    }
    catch (NeoAPI::NeoException& exc) {
        std::cout << "error: " << exc.GetDescription() << std::endl;
        result = 1;
    }
    catch (...) {
        std::cout << "oops, error" << std::endl;
        result = 1;
    }

    return result;
}
