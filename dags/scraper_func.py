#import librosa
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import timedelta

FEATS = {
  'co':  'cosc',
  'so2': 'so2smass',
  'no2': 'no2',
  'pm': 'pm2.5'
}


def scrape(feat, lat, long, **kwargs):
  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument('--no-sandbox')
  TIMEOUT = 35 #secs
  now = kwargs['execution_date'].replace(microsecond=0, second=0, minute=0)

  driver = webdriver.Chrome(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install(), options=options)
  
  year = str(now.year)
  month = str(now.month).zfill(2)
  day = str(now.day).zfill(2)
  hour = str(now.hour).zfill(2)
  
  url = f'https://earth.nullschool.net/#{year}/{month}/{day}/{hour}00Z/chem/surface/level/overlay={FEATS[feat]}/orthographic=-255.00,0.00,315/loc={long},{lat}'
  driver.get(url=url)

  WebDriverWait(driver, TIMEOUT).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, '[data-name="status-card"]')))
  raw_data = WebDriverWait(driver, TIMEOUT).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '[data-name="spotlight-b"]'))) \
            .text.split() # ['152', 'ppbv']
  
  data = float(raw_data[0])
  # convert to correct unit
  if(feat == 'co'): data /= 1000      # ppbv  -> ppm
  elif(feat == 'so2'): data /= 2.62   # ug/m3 -> ppb (for SO2)
  data = round(data, 2) # round to 2 dp for consistency
  
  timestamp = (now + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S') # +7 to TH timezone
  print('!!!!!!!!!!!!!!!!!!!!!!!!!')
  print(f'scraped @ {timestamp}: {feat} = {data}')
  print('!!!!!!!!!!!!!!!!!!!!!!!!!')
  driver.quit()
  return data