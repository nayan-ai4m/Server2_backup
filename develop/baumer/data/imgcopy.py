#!/usr/bin/env python3
import os
import shutil
import argparse
import random

def copy_images(input_dir, output_dir, num_images):
    # Ensure input directory exists
    if not os.path.exists(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist.")
        return
    
    # Create output directory if it doesn’t exist
    os.makedirs(output_dir, exist_ok=True)
    
    # List all image files (you can extend the list of extensions as needed)
    valid_exts = (".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp")
    images = [f for f in os.listdir(input_dir) if f.lower().endswith(valid_exts)]
    
    if not images:
        print("No images found in the input directory.")
        return
    
    # Limit number of images if requested number exceeds available
    num_to_copy = min(num_images, len(images))
    
    # Randomly select images to copy
    selected_images = random.sample(images, num_to_copy)
    
    # Copy selected images
    for img in selected_images:
        src = os.path.join(input_dir, img)
        dst = os.path.join(output_dir, img)
        shutil.copy2(src, dst)
    
    print(f"Copied {num_to_copy} images to '{output_dir}' successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy X number of images from one folder to another.")
    parser.add_argument("--input_dir", required=True, help="Path to the input directory containing images.")
    parser.add_argument("--output_dir", required=True, help="Path to the output directory to copy images to.")
    parser.add_argument("--num_images", type=int, required=True, help="Number of images to copy.")
    
    args = parser.parse_args()
    
    copy_images(args.input_dir, args.output_dir, args.num_images)
