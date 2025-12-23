/** \example getting_started.cpp
    This example describes the FIRST STEPS of handling Cam SDK.
    The given source code applies to handle one camera and image acquisition
    Copyright (c) by Baumer Optronic GmbH. All rights reserved, please see the provided license for full details.
*/

#include <iostream>
#include "neoapi/neoapi.hpp"

int main() {
    int result = 0;
    try {
        NeoAPI::Cam camera = NeoAPI::Cam();
        camera.Connect();
        camera.f().ExposureTime.Set(10000);

        NeoAPI::Image image = camera.GetImage();
        image.Save("getting_started.bmp");
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
