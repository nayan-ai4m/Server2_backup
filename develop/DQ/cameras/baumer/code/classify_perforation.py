import logging
import os
import time
import numpy as np
import cv2
import psycopg2
import tritonclient.grpc as grpcclient
from datetime import datetime


def transform_image(image):
    """
    Load and transform an image using PIL, OpenCV, and NumPy.

    Args:
        image_path (str): Path to the input image

    Returns:
        numpy.ndarray: Transformed and normalized image as a numpy array
    """
    try:
        # Load image using PIL and convert to RGB

        # Resize image using OpenCV
        image = cv2.resize(image, (224, 224))

        # Convert to float32 and normalize to [0, 1]
        image = image.astype(np.float32) / 255.0

        # Define mean and std for normalization (ImageNet values)
        mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
        std = np.array([0.229, 0.224, 0.225], dtype=np.float32)

        # Normalize image: (image - mean) / std
        image = (image - mean) / std

        # Transpose to (channels, height, width)
        image = np.transpose(image, (2, 0, 1))

        # Add batch dimension
        input_data = np.expand_dims(image, axis=0)

        return input_data
    except Exception as e:
        print(e)




try:
    triton_client = grpcclient.InferenceServerClient("localhost:9002")

    classes = [
        "bad","good"
    ]


    classify_inputs = []
    classify_outputs = []

    classify_inputs.append(grpcclient.InferInput("input", [1, 3, 224, 224], "FP32"))

    image_dir = os.listdir('/home/ai4m/develop/backup/develop/DQ/cameras/baumer/testing_crop_padding_MC17/')

    for image in image_dir:
        img = cv2.imread(os.path.join('/home/ai4m/develop/backup/develop/DQ/cameras/baumer/testing_crop_padding_MC17',image))
        

        cropped_image = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        #pil_image = Image.fromarray(cropped_image)
        #transformed_image = transform(pil_image)
        #input_data = np.array(transformed_image, dtype=np.float32)
        input_data = transform_image(cropped_image)
        #input_data = np.expand_dims(input_data, axis=0)
        classify_inputs[0].set_data_from_numpy(input_data)
        result = triton_client.infer(
            model_name="perforation_classify",
            inputs=classify_inputs,
            outputs=classify_outputs,
        )
        predictions = np.squeeze(result.as_numpy("output"))
        results = predictions.argmax()
        print(classes[results],image,predictions)


except Exception as e:
    print(e)
