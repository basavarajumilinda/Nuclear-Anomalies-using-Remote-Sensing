import requests
import numpy as np

import os


#Change the URLs to those specified 
def change_url(url, section, value):
    #split url into sections
    url_sections = url.split("&")
    #change each section
    for i in range(len(section)):
        url_sections[section[i]] = value[i]
    #join sections
    url = "&".join(url_sections)
    #return url
    return url


# Download .nc file from url
# def download_data(url, filename):
#     # open in binary mode
#     with open(filename, "wb") as file:
#         # get request
#         response = requests.get(url,timeout=100)
#         # write to file
#         file.write(response.content)

def download_data(url, filename):
    print(f"Attempting to download: {url}")
    response = requests.get(url, timeout=1000)

    print(f"Response code: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")

    if response.status_code == 200 and "netcdf" in response.headers.get("Content-Type", "").lower():
        with open(filename, "wb") as file:
            file.write(response.content)
        print(f"File saved to: {filename}")
    else:
        print("Failed to download a valid NetCDF file.")
        print("Response preview:")
        print(response.text[:500])  # show first 500 characters


# Changing the placeholder values to those specified
def present_values(values,year, month, day, north, west, east, south):
    values[0] = "year1=" + str(year)
    values[1] = "month1=" + str(month)
    values[2] = "day1=" + str(day)
    values[3] = "hr1=00%20Z"
    values[4] = "year2=" + str(year)
    values[5] = "month2=" + str(month)
    values[6] = "day2=" + str(day)
    values[7] = "hr2=00%20Z"
    values[8] = "region=Custom"
    values[9] = "area_north=" + str(north)
    values[10] = "area_west=" + str(west)
    values[11] = "area_east=" + str(east)
    values[12] = "area_south=" + str(south)

    return values


# Getting the YYYY, MM, DD from a date string
def get_date(date):
    year = date[0:4]
    month = date[5:7]
    day = date[8:10]
    return year, month, day
                                                                               

def get_region(region):

    # check if the N,S,W,E input values are negative due to how PSL processes coordinates
    if region[2][1] < 0:
        region[2][1] = region[2][1] + 180
    
    # check if region[0][1] is negative
    if region[0][0] < 0:
        region[0][0] = region[0][0] + 180
    
    # check if region[1][0] is negative
    if region[1][0] < 0:
        region[1][0] = region[1][0] + 180
    
    # check if region[0][1] is negative
    if region[0][1] < 0:
        region[0][1] = region[0][1] + 180

    north = abs(int(5 * round(float(region[2][1])/5))) + 5
    west = abs(int(5 * round(float(region[0][0])/5))) - 5
    east = abs(int(5 * round(float(region[1][0])/5))) + 5
    south = abs(int(5 * round(float(region[0][1])/5))) - 5

    return north, west, east, south

def round_down(num):
    num = int(num)
    if num % 5 == 0:
        return num
    else:
        return num - (num % 5)


def historical_values(values,year, month, day, north, west, east, south):
    values[0] = "year1=" + str(int(year) - 10)
    values[1] = "month1=" + str(month)
    values[2] = "day1=" + str(day)
    values[3] = "hr1=00%20Z"
    values[4] = "year2=" + str(year)
    values[5] = "month2=" + str(month)
    values[6] = "day2=" + str(day)
    values[7] = "hr2=00%20Z"
    values[8] = "region=Custom"
    values[9] = "area_north=" + str(north)
    values[10] = "area_west=" + str(west)
    values[11] = "area_east=" + str(east)
    values[12] = "area_south=" + str(south)

    return values

#function to call change_url and download_data
def get_data(url, filenamenew, filenameold, date, roi):
    #placeholder values
    values = ["year=2099", "month1=99", "day1=99", "hr1=00%99Z", "year2=2099", "month2=99", "day2=99", "hr2=00%99Z", "region=99", "area_north=99", "area_west=99", "area_east=99", "area_south=99"]
    sections = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

    year, month, day = get_date(date)
    north, west, east, south = get_region(roi)
    new_values = present_values(values, year, month, day, north, west, east, south)
    values = ["year=2099", "month1=99", "day1=99", "hr1=00%99Z", "year2=2099", "month2=99", "day2=99", "hr2=00%99Z", "region=99", "area_north=99", "area_west=99", "area_east=99", "area_south=99"]
    hist_values = historical_values(values, year, month, day, north, west, east, south)
    new_url = change_url(url, sections, new_values)
    hist_url = change_url(url, sections, hist_values)

    download_data(new_url, filenamenew)
    #download_data(hist_url,filenameold)

    return new_url

# Example call, in reality this would be called from a main indexing file
# A user/business would either click a NPP on an interactive map which
# would then call this function with the appropriate parameters or 
# a business would task the programme to run at specified schedules 
# for one specfic NPP over one region.
import os
print(os.getcwd())

get_data("https://psl.noaa.gov/cgi-bin/mddb2/plot.pl?doplot=0&varID=2713&fileID=0&itype=0&variable=olr&levelType=TOA&level_units=&level=TOA&timetype=day&fileTimetype=day&createAverage=1&year1=2011&month1=12&day1=31&hr1=00%20Z&year2=2012&month2=12&day2=31&hr2=00%20Z&region=All&area_north=90&area_west=0&area_east=360&area_south=-90&centerLat=0.0&centerLon=270.0","D:/Dissertation-2542000/RP3/OLR/testOLR.nc"
,"D:/Dissertation-2542000/RP3/OLR/testOLR.nc","2011-03-16", [[145,35],[145,35],[145,35],[145,35],[145,35]])










