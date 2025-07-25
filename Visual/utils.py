from PIL import Image, ImageOps
import cv2
import numpy as np
import matplotlib.pyplot as plt
from skimage import exposure
from skimage.exposure import match_histograms
import os

def gdal_call(path):
    #gdal_translate -of JPEG -outsize 1024 1024 input.tif output.jpeg
    os.system('gdal_translate -of JPEG -outsize 1024 1024' + ' "' + path + '" ' + '"' + path[:-4] + '.jpg'+ '"')


# function to zoom an image in by a factor of 1.75 in the top left quadrant
def zoom_image(path):

    # Open the image
    image = Image.open(path)
    # Calculate the coordinates for cropping
    crop_size = (int(image.width // 1.1), int(image.height // 1.1))  # New size after zooming
    left = 0  # Left coordinate for top left corner
    top = 0  # Top coordinate for top left corner
    right = crop_size[0]  # Right coordinate for top left corner
    bottom = crop_size[1]  # Bottom coordinate for top left corner

    # Crop the zoomed region from the original image
    zoomed_image = image.crop((left, top, right, bottom))

    # Resize the zoomed image back to the original size (1024x1024)
    zoomed_image = zoomed_image.resize((1024, 1024))

    # Convert the zoomed image to RGB mode
    zoomed_image = zoomed_image.convert("RGB")

    # Save the zoomed image
    zoomed_image.save("D:/Dissertation-2542000/RP3/Visual/ZapPhotos/FINALTESTSJK/20TH/Zoom/zoomed_20TH_1_1.png")

    return zoomed_image


def hist_match(path_pre, path_post):
    #source: https://scikit-image.org/docs/stable/auto_examples/color_exposure/plot_histogram_matching.html

    reference = cv2.imread(path_pre,1)
    image = cv2.imread(path_post,1)

    matched = match_histograms(image, reference, channel_axis=-1)

    # save the matched image
    cv2.imwrite("D:/Dissertation-2542000/RP3/Visual/ZapPhotos/FINALTESTSJK/20TH/HIST/hist_20TH_1_1.png", matched)


    # verification:
    
    # fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3, figsize=(8, 3),
    #                                     sharex=True, sharey=True)
    # for aa in (ax1, ax2, ax3):
    #     aa.set_axis_off()

    # ax1.imshow(image)
    # ax1.set_title('Source')
    # ax2.imshow(reference)
    # ax2.set_title('Reference')
    # ax3.imshow(matched)
    # ax3.set_title('Matched')

    # plt.tight_layout()
    # plt.show()

    return matched

if __name__ == "__main__":
    

    hist_match("D:/Dissertation-2542000/RP3/Visual/ZapPhotos/FINAL_TESTS/histogram_image/hurricane-harvey_00000386_pre_disaster.png", "D:/Dissertation-2542000/RP3/Visual/ZapPhotos/FINALTESTSJK/20TH/Original/20220820_081643_09_2490_3B_Visual_clip.png")



    zoom_image("D:/Dissertation-2542000/RP3/Visual/ZapPhotos/FINALTESTSJK/20TH/HIST/hist_20TH_1_1.png")
