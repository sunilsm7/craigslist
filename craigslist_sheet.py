import csv
import logging
import re
import os

from datetime import datetime, date

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


from bs4 import BeautifulSoup
import urllib.request
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

scope = [
    "https://spreadsheets.google.com/feeds",
    'https://www.googleapis.com/auth/spreadsheets',
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

BASE_URL = "https://{location}.craigslist.org/search/jjj?query={keyword}&sort=rel"


class CraiglistScraperException(Exception):
    pass


class CraiglistScraper(object):
    def __init__(self):
        self.base_url = BASE_URL
        self.keywords = []
        self.sheet_id = "1d7hS1n4mLsgXK722aDfiXx1H7x38Ih8VwnPuV_ku7uM"
        self.keywords_sheet_name = "CraigslistKeywords"
        self.driver = None
        self.current_date = str(date.today())
        self.service_account_path = "D:\\projects\\web-crawlers\\craigslist\\credentials.json"
        self.delay= 3
        self.creds = Credentials.from_service_account_file(self.service_account_path, scopes=scope)
        self.gc_client = gspread.authorize(self.creds)

    def read_sheets_keywords(self):
        """
        read keywords from google sheet file
        """
        sheet = self.gc_client.open_by_key(self.sheet_id)
        worksheet = sheet.worksheet(self.keywords_sheet_name)
        self.keywords = worksheet.get_all_records()
        return self.keywords

    def update_google_spread_sheet(self, results):
        """
        update extracted results to google sheet
        """
        if results:
            sheet = self.gc_client.open_by_key(self.sheet_id).sheet1
            sheet.append_rows(results)

    def crawl(self):
        """
        crawl sites with keyword and location
        """
        crawl_keywords = self.read_sheets_keywords()

        for item in crawl_keywords: 
            keyword = item.get('Keyword')
            location = item.get('Location')
            domain = item.get('Domain')

            self.crawl_sites(keyword, location)

    def crawl_keyword(self):
        """
        Crawl by keyword.
        ex. for one keyword we can crawl all the available locations
        """
        crawl_keywords = self.read_sheets_keywords()
        keywords = [sub['Keyword'] for sub in crawl_keywords if sub['Keyword']]
        locations = [sub['Location'] for sub in crawl_keywords if sub['Location']] 

        for keyword in keywords:
            for location in locations:
                self.crawl_sites(keyword, location)


    def crawl_sites(self, keyword, location):
        results = []
        # instantiate a chrome options object so you can set the size and headless preference
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")


        driver = webdriver.Chrome(options=chrome_options)
        url = self.base_url.format(location=location, keyword=keyword)
        driver.get(url)

        try:
            wait = WebDriverWait(driver, self.delay)
            wait.until(EC.presence_of_element_located((By.ID, 'searchform')))
            logger.info('Page is ready')

            links = driver.find_elements_by_css_selector("a.result-title")

            for link in links:
                job_url = link.get_attribute('href')
                job_title = link.get_attribute('text')
                search_location = r'{location}'.format(location=location)

                # exclude local results
                if re.search(search_location, job_url.lower()):
                    job_post = [self.current_date, keyword, location, job_title, '', job_url]
                    results.append(job_post)

            # push results to google sheets
            self.update_google_spread_sheet(results)

        except TimeoutException:
            logger.error('Loading took too much time')

        # close driver
        driver.close()
        return results

    def crawl_bs4(self):
        crawl_keywords = self.read_sheets_keywords()

        for item in crawl_keywords: 
            keyword = item.get('keyword')
            location = item.get('location')
            self.extract_post_urls(keyword, location)

    def extract_post_urls(self, keyword, location):
        url = url = self.base_url.format(keyword=keyword, location=location)
        url_list = []
        results = []
        html_page = urllib.request.urlopen(url)
        soup = BeautifulSoup(html_page,'lxml')

        for link in soup.findAll("a", {"class":"result-title hdrlnk"}):
            job_url = link['href']
            job_title = link.get_text()
            job_post = [self.current_date, keyword, location, job_title, '', job_url]
            url_list.append(job_post)

        # push results to google sheets
        self.update_google_spread_sheet(results)
        return url_list


def main():      

    scraper= CraiglistScraper()
    keywords = scraper.read_sheets_keywords()
    scraper.crawl_keyword()


if __name__ == '__main__':
    main()
