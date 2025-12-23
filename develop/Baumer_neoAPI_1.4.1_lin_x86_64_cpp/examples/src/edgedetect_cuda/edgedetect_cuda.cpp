/** \example edgedetect_cuda.cpp
    This example show the pro and cons of gpu usage for image processing
    by acquire images with a camera and run an edge detection on this images.
    Firstly the processing is done on the cpu. Then we switch to the gpu.
    As last step we use shared buffers to prevent the data transfer to the gpu.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

/* To support Cuda you have to install the nvidia driver for your platform.
   On Debian based Linux distributions use e.g.: "apt install nvidia-driver-xxx" to install the nvidia binary driver package */
#include <iostream>
#include <thread>
#include <vector>
#include <map>
#include "opencv2/imgproc.hpp"
#include "opencv2/imgcodecs.hpp"
#include "opencv2/highgui.hpp"
#include "opencv2/core/cuda.hpp"
#include "opencv2/cudaimgproc.hpp"
#include "opencv2/cudafilters.hpp"
#include "neoapi/neoapi.hpp"

enum class MemoryMode {
    cpu = 0,
    gpu = cv::cuda::HostMem::AllocType::PAGE_LOCKED,
    shared = cv::cuda::HostMem::AllocType::SHARED
};

class CamBuffer : public NeoAPI::BufferBase {
 public:
    CamBuffer(int width, int height, MemoryMode memtype) {
        // alloc memory with opencv
        if (memtype == MemoryMode::cpu) {
            cpu_mat_ = cv::Mat(cv::Size(width, height), CV_8UC1);
        } else {
            gpu_memory_ = cv::cuda::HostMem(cv::Size(width, height), CV_8UC1,
                static_cast<cv::cuda::HostMem::AllocType>(memtype));
            cpu_mat_ = gpu_memory_.createMatHeader();
            if (memtype == MemoryMode::gpu) {
                gpu_mat_ = cv::cuda::GpuMat(gpu_memory_);
            } else {
                gpu_mat_ = gpu_memory_.createGpuMatHeader();
            }
        }
        memtype_ = memtype;
        // announce buffer to neoapi
        RegisterMemory(cpu_mat_.data, cpu_mat_.total());
    }
    ~CamBuffer() {
        UnregisterMemory();
    }
    cv::cuda::HostMem gpu_memory_;
    cv::Mat cpu_mat_;
    cv::cuda::GpuMat gpu_mat_;
    MemoryMode memtype_;
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
        // prepare cuda kernels to applicate filters
        gauss_filter_ = cv::cuda::createGaussianFilter(CV_8UC1, CV_8UC1, cv::Size(5, 5), 0);
        sobel_filter_ = cv::cuda::createSobelFilter(CV_8UC1, CV_8UC1, 1, 1, 5);

        SetupBuffers(3, memtype);
        camera_.SetUserBufferMode();
    }

    // single edge detection on a given image
    void Detect(const NeoAPI::Image& image, bool show_image) {
        CamBuffer *buf = image.GetUserBuffer<CamBuffer*>();
        if (MemoryMode::cpu == buf->memtype_) {
            if (NeoAPI::PixelFormat::BayerRG8 == pixel_format_) {
                cv::cvtColor(buf->cpu_mat_, cpu_grey_mat_, cv::COLOR_BayerRG2GRAY);
            } else {
                cpu_grey_mat_ = buf->cpu_mat_;
            }
            cv::GaussianBlur(cpu_grey_mat_, cpu_gauss_mat_, cv::Size(5, 5), 0);
            cv::Sobel(cpu_gauss_mat_, cpu_sobel_mat_, cpu_sobel_mat_.depth(), 1, 1, 5);
        } else {
            if (MemoryMode::gpu == buf->memtype_) {
                buf->gpu_mat_.upload(buf->cpu_mat_);
            }
            if (NeoAPI::PixelFormat::BayerRG8 == pixel_format_) {
                cv::cuda::cvtColor(buf->gpu_mat_, gpu_grey_mat_, cv::COLOR_BayerRG2GRAY);
            } else {
                gpu_grey_mat_ = buf->gpu_mat_;
            }
            gauss_filter_->apply(gpu_grey_mat_, gpu_gauss_mat_);
            sobel_filter_->apply(gpu_gauss_mat_, gpu_sobel_mat_);
        }
        if (show_image) {
            if (MemoryMode::cpu != buf->memtype_) {
                gpu_sobel_mat_.download(cpu_sobel_mat_);
            }
            cv::imshow(identifier_, cpu_sobel_mat_);
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
        // due to show image the cpu sobel matrix is always needed
        cpu_sobel_mat_ = cv::Mat(cv::Size(width, height), CV_8UC1);
        if (memtype == MemoryMode::cpu) {
            cpu_grey_mat_ = cv::Mat(cv::Size(width, height), CV_8UC1);
            cpu_gauss_mat_ = cv::Mat(cv::Size(width, height), CV_8UC1);
        } else  {
            gpu_grey_mat_ = cv::cuda::GpuMat(cv::Size(width, height), CV_8UC1);
            gpu_gauss_mat_ = cv::cuda::GpuMat(cv::Size(width, height), CV_8UC1);
            gpu_sobel_mat_ = cv::cuda::GpuMat(cv::Size(width, height), CV_8UC1);
        }
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
    cv::Mat cpu_grey_mat_;
    cv::Mat cpu_gauss_mat_;
    cv::Mat cpu_sobel_mat_;
    cv::cuda::GpuMat gpu_grey_mat_;
    cv::cuda::GpuMat gpu_gauss_mat_;
    cv::cuda::GpuMat gpu_sobel_mat_;
    cv::Ptr<cv::cuda::Filter> gauss_filter_;
    cv::Ptr<cv::cuda::Filter> sobel_filter_;
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
    if (cv::cuda::getCudaEnabledDeviceCount() > 0) {
        (*memtypes)[MemoryMode::gpu] = "gpu";
        if (cv::cuda::DeviceInfo().unifiedAddressing()) {
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

    // look if the gpu supports cuda and shared memory
    GetGpuCapabilities(&memtypes);

    // find all connected cameras
    FindDevices(&devices);

    // edge detection processing on all connected cameras
    RunDetection(devices, memtypes, show_images);

    // cleanup
    FreeDevices(&devices);

    return 0;
}
