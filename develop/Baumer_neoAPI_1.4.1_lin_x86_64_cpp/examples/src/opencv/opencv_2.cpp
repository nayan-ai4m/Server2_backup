/** \example opencv.cpp
    A simple Program for grabbing video from Baumer camera and converting it to opencv images.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/
#include <thread>
#include <stdio.h>
#include <iostream>
#include <opencv2/opencv.hpp>
#include <opencv2/highgui.hpp>
#include "neoapi/neoapi.hpp"
#include <zmq.hpp>
#include <string>



class TestNeoImageCallback : public NeoAPI::NeoImageCallback {
public:
	TestNeoImageCallback() : context_(1), publisher_(context_, ZMQ_PUB) {
        // Bind the publisher to a TCP endpoint (e.g., port 5555)
        publisher_.bind("tcp://*:5554");
        std::cout << "ZeroMQ publisher bound to tcp://*:5555" << std::endl;
    }
    
	virtual void ImageCallback(const NeoAPI::Image& image) {
        
	    std::cout << "Received image: " << image.GetImageID() <<
            " Size: " << image.GetSize() << " Height: " << image.GetHeight() <<
            " Width: " << image.GetWidth() << " PixelFormat: " << image.GetPixelFormat() << std::endl;
	int width = static_cast<int>(image.GetWidth());
        int height = static_cast<int>(image.GetHeight());

	cv::Mat img(cv::Size(width, height),CV_8UC3, image.GetImageData(), cv::Mat::AUTO_STEP);
	std::vector<uchar> encodedImage;
        bool success = cv::imencode(".jpg",img, encodedImage, {cv::IMWRITE_JPEG_QUALITY, 100});
        if (!success) {
            std::cerr << "Failed to encode image" << std::endl;
            return;
        }

        // Create and send ZeroMQ message with encoded data
        zmq::message_t message(encodedImage.data(), encodedImage.size());
        publisher_.send(message, zmq::send_flags::none);
	std::cout<<"published image \n";
	//char filename[256];
        //snprintf(filename,256,"/home/ai4m/develop/baumer_cap/18/%ld.png",image.GetTimestamp());
        //cv::imwrite(filename,img);
    }
private:
    zmq::context_t context_;           // ZeroMQ context (1 = one I/O thread)
    zmq::socket_t publisher_;
};


class TestNeoEventCallback : public NeoAPI::NeoEventCallback {
 public:
    virtual void EventCallback(const NeoAPI::NeoEvent& event) {
        std::cout << "Received event: " << event.GetName() << " at: " <<
            event.GetTimestamp() << " id: 0x" << std::hex << event.GetId() << std::dec << std::endl;
    }
};
int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
        camera.Connect("700011046057");
        NeoAPI::NeoStringList event_names;
	event_names = camera.GetAvailableEvents();              // get list of available events
	for (auto event_name : event_names) {
    	std::cout << "Event name: " << event_name << std::endl;
	}
	camera.f().ExposureTime.Set(4000);
	camera.f().Gain.Set(1.07);
	camera.f().AcquisitionFrameRateEnable = false;
	//camera.f().AcquisitionFrameRate.Set(2.0);
        camera.f().TriggerMode = NeoAPI::TriggerMode::On;       // bring camera in TriggerMode
	//camera.f().TriggerSource = NeoAPI::TriggerSource::Software;
	camera.f().TriggerSource = NeoAPI::TriggerSource::Line1;
	camera.f().TriggerActivation.Set(NeoAPI::TriggerActivation::RisingEdge); 
	camera.f().TriggerDelay.Set(5000);
	
	TestNeoImageCallback callback;
	
	camera.EnableImageCallback(callback);
	int type = CV_8U;
        bool isColor = true;
        if (camera.f().PixelFormat.GetEnumValueList().IsReadable("BGR8")) {
            camera.f().PixelFormat.SetString("BGR8");
            type = CV_8UC3;
            isColor = true;
        } else if (camera.f().PixelFormat.GetEnumValueList().IsReadable("Mono8")) {
            camera.f().PixelFormat.SetString("Mono8");
            type = CV_8UC1;
            isColor = false;
        } else {
            std::cout << "no supported pixel format";
            return 0;  // Camera does not support pixelformat
        }
        int width = static_cast<int>(camera.f().Width);
        int height = static_cast<int>(camera.f().Height);
	while (1) 
	{
	    std::this_thread::sleep_for(std::chrono::milliseconds(600));
	    //camera.f().TriggerSoftware.Execute();
};
	camera.DisableImageCallback();
	//MJPEGStreamer streamer;
	//streamer.start(8003);
        
	/*
	int type = CV_8U;
        bool isColor = true;
        if (camera.f().PixelFormat.GetEnumValueList().IsReadable("BGR8")) {
            camera.f().PixelFormat.SetString("BGR8");
            type = CV_8UC3;
            isColor = true;
        } else if (camera.f().PixelFormat.GetEnumValueList().IsReadable("Mono8")) {
            camera.f().PixelFormat.SetString("Mono8");
            type = CV_8UC1;
            isColor = false;
        } else {
            std::cout << "no supported pixel format";
            return 0;  // Camera does not support pixelformat
        }
        int width = static_cast<int>(camera.f().Width);
        int height = static_cast<int>(camera.f().Height);

        // Define the codec and create VideoWriter object.The output is stored in 'outcpp.avi' file.
        // Define the fps to be equal to 10. Also frame size is passed.
        // cv::VideoWriter video("outcpp.avi", cv::VideoWriter::fourcc('M', 'J', 'P', 'G'), 10,
        // cv::VideoWriter video("outcpp.avi", cv::VideoWriter::fourcc('D', 'I', 'V', 'X'), 10,
	std::vector<int> params = {cv::IMWRITE_JPEG_QUALITY, 90};
        while (streamer.isRunning()) {
            NeoAPI::Image image = camera.GetImage();
            if (!image.IsEmpty()){
	    cv::Mat img(cv::Size(width, height), type, image.GetImageData(), cv::Mat::AUTO_STEP);
	    std::vector<uchar> buff_bgr;
	    char filename[256];
	    snprintf(filename,256,"/home/ai4m/develop/baumer_cap/%ld.png",image.GetImageID());
	    cv::imwrite(filename,img);
	    cv::imencode(".jpg", img, buff_bgr, params);
            streamer.publish("/bgr", std::string(buff_bgr.begin(), buff_bgr.end()));
	    }
        }
    	streamer.stop();
	*/
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
