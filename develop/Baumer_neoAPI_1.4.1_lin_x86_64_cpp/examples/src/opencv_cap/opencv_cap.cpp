/** \example opencv.cpp
    A simple Program for grabbing video from Baumer camera and converting it to opencv images.
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

#include <stdio.h>
#include <iostream>
#include <opencv2/opencv.hpp>
#include <opencv2/highgui.hpp>
#include "neoapi/neoapi.hpp"
#include <nadjieb/mjpeg_streamer.hpp>

using MJPEGStreamer = nadjieb::MJPEGStreamer;

int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
        camera.Connect("700011047484");
	camera.f().TriggerMode = NeoAPI::TriggerMode::Off;
        camera.f().ExposureTime.Set(4000);
	camera.f().Gain.Set(1.0);
	camera.f().AcquisitionFrameRateEnable = true;
	camera.f().AcquisitionFrameRate.Set(2.0);
        MJPEGStreamer streamer;
	streamer.start(8256);
        int type = CV_8U;
        bool isColor = true;
        if (camera.f().PixelFormat.GetEnumValueList().IsReadable("BayerRG8")) {
            camera.f().PixelFormat.SetString("BayerRG8");
            type = CV_8UC1;
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
	cv::Mat BGRmat(cv::Size(width, height), CV_8UC3);
        // Define the codec and create VideoWriter object.The output is stored in 'outcpp.avi' file.
        // Define the fps to be equal to 10. Also frame size is passed.
        // cv::VideoWriter video("outcpp.avi", cv::VideoWriter::fourcc('M', 'J', 'P', 'G'), 10,
        // cv::VideoWriter video("outcpp.avi", cv::VideoWriter::fourcc('D', 'I', 'V', 'X'), 10,
	std::vector<int> params = {cv::IMWRITE_JPEG_QUALITY, 90};
        while (streamer.isRunning()) {
            NeoAPI::Image image = camera.GetImage();
            if (!image.IsEmpty()){
	    cv::Mat img(cv::Size(width, height), type, image.GetImageData(), cv::Mat::AUTO_STEP);
	    cv::cvtColor(img, BGRmat, cv::COLOR_BayerBG2BGR);
	    std::vector<uchar> buff_bgr;
	    char filename[256];
	    //snprintf(filename,256,"/home/ai4m/develop/baumer_cap/%ld.png",image.GetImageID());
	    //cv::imwrite(filename,img);
	    cv::imencode(".png", BGRmat, buff_bgr);
            streamer.publish("/bgr", std::string(buff_bgr.begin(), buff_bgr.end()));
	    }
        }
    	streamer.stop();
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
