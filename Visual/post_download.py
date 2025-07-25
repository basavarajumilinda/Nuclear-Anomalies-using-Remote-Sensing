import os
from datetime import datetime, timezone
#from histmatch import *
from utils import hist_match


# Directory should be the Visual folder

def compare_dates(filenames):
    print(filenames)
    print(filenames[0])
    ymd = [] #[[year, month, day], [year, month, day]]
    time = [] #[[hour, minute, second], [hour, minute, second]]

    for filename in filenames:
            ymd_str = filename[0:8]
            year = int(ymd_str[0:4])
            month = int(ymd_str[4:6])
            day = int(ymd_str[6:8])
            ymd.append([year, month, day])

            time_str = filename[9:15]
            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])
            time.append([hour, minute, second])

    # date_utc = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    dt1 = datetime(ymd[0][0], ymd[0][1], ymd[0][2], time[0][0], time[0][1], time[0][2], tzinfo=timezone.utc)
    dt2 = datetime(ymd[1][0], ymd[1][1], ymd[1][2], time[1][0], time[1][1], time[1][2], tzinfo=timezone.utc)

    #sorting to [pre, post]
    if dt1 > dt2: # date 1 is post date 2
        return [filenames[1], filenames[0]]
    else:
        return [filenames[0], filenames[1]] # date 2 is post date 1 



def tifconvert(id):
    download_dir = os.path.join(os.getcwd(), 'downloads', id, 'PSScene')
    print('download dir: ', download_dir)

    for file in os.listdir(download_dir):
        if file.endswith(".tif"):
            print(file)
            filepath = os.path.join(download_dir, file)
            print('filepath:',filepath)
            # os.system gdal_translate -of JPEG -outsize 1024 1024 input.tif output.jpeg
            os.system('gdal_translate -of JPEG -outsize 1024 1024' + ' "' + filepath + '" ' + '"' + filepath[:-4] + '.jpg'+ '"')
            
    
    #copy the new .tif files into a new /output directory
    output_dir = os.path.join(os.getcwd(), 'outputs')
    print('output dir: ', output_dir)
    # copy the .tif files into the new directory
    for file in os.listdir(download_dir):
        if file.endswith(".jpg"):
            output_dir = os.path.join(os.getcwd() , 'outputs', file)
            input_dir = os.path.join(os.getcwd(), 'downloads', id, 'PSScene', file)

            command = 'copy' + ' "' + input_dir + '" ' + '"' + output_dir + '"'

            # Verification purposes
            # print('this is the file: ', file)
            # print('this is the output dir: ', output_dir)
            # print('this is the input dir: ', input_dir)
            # print('This is the command: ', command)
            
            os.system(command)
            os.system('del' + ' "' + input_dir + '"')



def rename_files(directory):
    filenames = os.listdir(directory)
    files = []

    for filename in filenames:
        if filename.endswith(".jpg"):
            files.append(filename)
            
    [pre, post] = compare_dates(files)
    print('this is pre: ', pre)

    # Rename the files
    os.rename(os.path.join(directory, pre), os.path.join(directory, 'x_pre_disaster.jpg'))
    os.rename(os.path.join(directory, post), os.path.join(directory, 'x_post_disaster.jpg'))

def getId():
    # be in the visual folder
    path = os.getcwd()
    path_downloads = os.path.join(path, 'downloads')
    id = os.listdir(path_downloads)[0]
    print('id: ', id)

    return id

def histmatch_save(dir):
    pre = os.path.join(dir, 'x_pre_disaster.jpg')
    post = os.path.join(dir, 'x_post_disaster.jpg')
    matched = hist_match(pre, post)
    cv2.imwrite(pre, matched)

def postdownload(orderid):
    # id = getId()
    id=orderid
    print(id)
    tifconvert(id)
    renaming_path = os.path.join(os.getcwd(), 'outputs')
    rename_files(renaming_path)
    histmatch_save(renaming_path)

