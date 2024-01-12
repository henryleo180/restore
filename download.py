import os
import time
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select


def wait_for_downloads_to_finish(download_directory):
    seconds = 0
    downloads_still_in_progress = True

    while downloads_still_in_progress and seconds < 300:
        existing_files = set(os.listdir(download_directory))
        time.sleep(1)
        seconds += 1
        downloads_still_in_progress = False
        current_files = set(os.listdir(download_directory))
        new_files = current_files.difference(existing_files)

        for file_name in os.listdir(download_directory):
            if file_name.endswith('.crdownload'):
                downloads_still_in_progress = True
        for file_name in new_files:
            if file_name.endswith('.zip'):
                print(f"{file_name} download completed")
                extract_directory = os.path.join(download_directory, 'downfiles')
                os.makedirs(extract_directory, exist_ok=True)
                with zipfile.ZipFile(os.path.join(download_directory, file_name), 'r') as zip_ref:
                    zip_ref.extractall(extract_directory)

    if downloads_still_in_progress:
        print("Downloads did not finish in 5 minutes.")


# get the current path
current_directory = os.path.dirname(os.path.realpath(__file__))

# create directory named "data" if no exited
data_directory = os.path.join(current_directory, 'data')
os.makedirs(data_directory, exist_ok=True)

# set chrome download, set the download dictionary data
options = webdriver.ChromeOptions()
prefs = {'download.default_directory': data_directory}
options.add_experimental_option('prefs', prefs)
options.add_argument("--headless")
options.add_argument("--log-level=3")

# create a new chrome instance
driver = webdriver.Chrome(options=options)

# visti website
driver.get('https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx')

# set the file
file_select_element = driver.find_element(By.NAME, 'ctl00$MainContentHolder$ListBox1')
file_select = Select(file_select_element)
file_select.select_by_value('ReportingSeriesSubsetSchedulesFourPeriods')

# set the year
years = ['108','115','121','126', '131', '136']
for year in years:
    year_select_element = driver.find_element(By.NAME, 'ctl00$MainContentHolder$DatesDropDownList')
    year_select = Select(year_select_element)
    year_select.select_by_value(year)

    # press download
    download_button = driver.find_element(By.NAME, 'ctl00$MainContentHolder$TabStrip1$Download_0')
    download_button.click()

# waiy for download
wait_for_downloads_to_finish(data_directory)

# close chrome
driver.quit()
