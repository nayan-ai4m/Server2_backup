/** \example edgedetect_opencl.cpp
    This example show the pro and cons of gpu usage for image processing
    by acquire images with a camera and run an edge detection on this images.
    Firstly the processing is done on the cpu. Then we switch to the gpu.
    As last step we use shared buffers to prevent the data transfer to the gpu.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

/* To support OpenCL you have to install the driver for your platform.
   On Debian based Linux distributions use e.g.: "apt install beignet-opencl-icd" for intel igpu and check
   that the required devices in /dev/dri are in group video.
*/

#include <iostream>
#include <thread>
#include <vector>
#include <map>
#include "opencv2/imgproc.hpp"
#include "opencv2/imgcodecs.hpp"
#include "opencv2/highgui.hpp"
#include "opencv2/core/ocl.hpp"
#include "neoapi/neoapi.hpp"

enum class MemoryMode {
    cpu = cv::UMatUsageFlags::USAGE_ALLOCATE_HOST_MEMORY,
    gpu = cv::UMatUsageFlags::USAGE_ALLOCATE_DEVICE_MEMORY,
    shared = cv::UMatUsageFlags::USAGE_ALLOCATE_SHARED_MEMORY
};

class CamBuffer : public NeoAPI::BufferBase {
 public:
    CamBuffer(int width, int height, MemoryMode memtype) {
        // following approach grant opencl access automatically
        // alloc memory with opencv
        cpu_mat_ = cv::Mat(cv::Size(width, height), CV_8UC1);
        // map to UMat with specified memory type
        gpu_mat_ = cpu_mat_.getUMat(cv::ACCESS_READ, static_cast<cv::UMatUsageFlags>(memtype));
        // announce buffer to neoapi
        RegisterMemory(cpu_mat_.data, cpu_mat_.total());
    }
    ~CamBuffer() {
        UnregisterMemory();
    }
    cv::Mat cpu_mat_;
    cv::UMat gpu_mat_;
};

class EdgeDetector {
 public:
    explicit EdgeDetector(NeoAPI::NeoString serialnumber) {
        camera_.Connect(serialnumber);
        camera_.f().ExposureTime.Set(2500);
        // cv mats will created by width and height -> there is no space for chunk -> disable chunk
        camera_.DisableChunk();
        try {
            camera_.f().PixelFormat.Set(NeoAPI::PixelFormat::BayerRG8);
        } catch (NeoAPI::FeatureAccessException&) {
            camera_.f().PixelFormat.Set(NeoAPI::PixelFormat::Mono8);
        }
        pixel_format_ = camera_.f().PixelFormat.Get();
        identifier_ = serialnumber;
    }

    ~EdgeDetector() {
        Stop();
        FreeCamBuffers();
    }

    // setups the edge detector to do processing with the requested type
    void Setup(MemoryMode memtype) {
        cv::ocl::setUseOpenCL(MemoryMode::cpu != memtype);
        if (cv::ocl::Device::getDefault().hostUnifiedMemory()) {  // do not use svm functions if this failes
            try {
                cv::ocl::Context::getDefault().setUseSVM(MemoryMode::shared == memtype);
            }
            catch(...) {
                memtype = MemoryMode::cpu;
                std::cout << "SVM Error: falling back to cpu memory!" << std::endl;
            }
        }

        SetupBuffers(3, memtype);
        camera_.SetUserBufferMode();
    }

    // single edge detection on a given image
    void Detect(const NeoAPI::Image& image, bool show_image) {
        cv::UMat *img_mat = &(image.GetUserBuffer<CamBuffer*>()->gpu_mat_);
        if (NeoAPI::PixelFormat::BayerRG8 == pixel_format_) {
            cv::cvtColor(*img_mat, grey_mat_, cv::COLOR_BayerRG2GRAY);
        } else {
            grey_mat_ = *img_mat;
        }
        cv::GaussianBlur(grey_mat_, gauss_mat_, cv::Size(5, 5), 0);
        cv::Sobel(gauss_mat_, sobel_mat_, sobel_mat_.depth(), 1, 1, 5);
        if (show_image) {
            cv::imshow(identifier_, sobel_mat_);
            cv::pollKey();
        }
        ++frames_;
    }

    // returns the number of processed images since last call
    size_t ProcessedFrames() {
        size_t ret = frames_;
        frames_ = 0;
        return ret;
    }

    // return the cameras serial number
    const cv::String& GetIdentifier() {
        return identifier_;
    }

    // starts a seperate thread that will do edge detection continouosly
    void Start(bool show_images) {
        run_ = true;
        detect_thread_ = std::thread(&EdgeDetector::Detect_, this, show_images);
    }

    // stops a previous started continouosly edge detection
    void Stop() {
        run_ = false;
        if (detect_thread_.joinable()) {
            detect_thread_.join();
        }
    }

 private:
    void FreeCamBuffers() {
        while (!buffers_.empty()) {
            delete buffers_.back();
            buffers_.pop_back();
        }
    }

    void SetupBuffers(size_t count, MemoryMode memtype) {
        int width = static_cast<int>(camera_.f().Width.Get());
        int height = static_cast<int>(camera_.f().Height.Get());
        FreeCamBuffers();
        for (size_t i = 0; i < count; ++i) {
            buffers_.push_back(new CamBuffer(width, height, memtype));
            camera_.AddUserBuffer(buffers_.back());
        }
        // allocate processing matrices because operations cannot run in place
        // some opencv versions use the wrong constructor -> create the mats objects with explicit memory type
        grey_mat_ = cv::UMat();
        gauss_mat_ = cv::UMat();
        sobel_mat_ = cv::UMat();
        grey_mat_.create(cv::Size(width, height), CV_8UC1, static_cast<cv::UMatUsageFlags>(memtype));
        gauss_mat_.create(cv::Size(width, height), CV_8UC1, static_cast<cv::UMatUsageFlags>(memtype));
        sobel_mat_.create(cv::Size(width, height), CV_8UC1, static_cast<cv::UMatUsageFlags>(memtype));
    }

    void Detect_(bool show_images) {
        try {
            while (run_) {
                NeoAPI::Image image = camera_.GetImage();
                if (image.IsEmpty()) {
                    std::cout << identifier_ << " Error during acquisition!" << std::endl;
                    break;
                } else {
                    Detect(image, show_images);
                }
            }
            if (show_images) {
                cv::destroyWindow(identifier_);
            }
        } catch (NeoAPI::NeoException& exc) {
            std::cout << identifier_ << " error: " << exc.GetDescription() << std::endl;
        } catch (cv::Exception& exc) {
            std::cout << identifier_ <<  "cv error:" << exc.msg << std::endl;
        }
    }

    NeoAPI::Cam camera_;
    std::vector<CamBuffer*> buffers_;
    cv::String identifier_;
    cv::UMat grey_mat_;
    cv::UMat gauss_mat_;
    cv::UMat sobel_mat_;
    std::thread detect_thread_;
    size_t frames_ {0};
    NeoAPI::PixelFormat pixel_format_;
    bool run_ {false};
};

void PrintMetrics(const std::vector<EdgeDetector*>& devices, size_t duration) {
    for (size_t secs = 0; secs < duration; ++secs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // print every second metrics
        for (auto device : devices) {
            std::cout << device->GetIdentifier() << " fps: " << device->ProcessedFrames() << std::endl;
        }
    }
}

void FindDevices(std::vector<EdgeDetector*> *devices) {
    for (auto device : NeoAPI::CamInfoList::Get()) {
        try {
            devices->push_back(new EdgeDetector(device.GetSerialNumber()));
        }
        catch (NeoAPI::NeoException& exc) {
            std::cout << exc.GetDescription() << std::endl;
        }
    }
    std::cout << devices->size() << " device(s) connected!" << std::endl;
}

void GetGpuCapabilities(std::map<MemoryMode, std::string> *memtypes) {
    (*memtypes)[MemoryMode::cpu] = "cpu";
    if (cv::ocl::haveOpenCL()) {
        (*memtypes)[MemoryMode::gpu] = "gpu";
        if (cv::ocl::Device::getDefault().hostUnifiedMemory()) {
            (*memtypes)[MemoryMode::shared] = "gpu with shared memory";
        }
    }
}

void RunDetection(const std::vector<EdgeDetector*> &devices,
                  const std::map<MemoryMode, std::string> &memtypes,
                  bool show_images) {
    if (devices.size()) {
        for (auto memtype : memtypes) {
            std::cout << "Next run will be processed on " << memtype.second << std::endl;
            for (auto device : devices) {
                device->Setup(memtype.first);
            }

            for (auto device : devices) {
                device->Start(show_images);
            }

            // run the detection for given time in seconds and print status informations
            PrintMetrics(devices, 5);

            for (auto device : devices) {
                device->Stop();
            }
        }
    }
}

void FreeDevices(std::vector<EdgeDetector*> *devices) {
    while (devices->size()) {
        delete devices->back();
        devices->pop_back();
    }
}

int main(int argc, char *argv[]) {
    /* Showing the images have a high impact on processing speed.
       For better comparision show_images should be disabled. */
    bool show_images = ((argc > 1) && argv);
    std::map<MemoryMode, std::string> memtypes;
    std::vector<EdgeDetector*> devices;

    // look if the gpu supports opencl and shared memory
    GetGpuCapabilities(&memtypes);

    // find all connected cameras
    FindDevices(&devices);

    // edge detection processing on all connected cameras
    RunDetection(devices, memtypes, show_images);

    // cleanup
    FreeDevices(&devices);

    return 0;
}
