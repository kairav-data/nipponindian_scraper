import logging
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime, time
import time as tm
import os

# Set up logging
logging.basicConfig(
    filename='nipponindia_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Log the start of the script
logging.info("Script started")

cookies = {
    '_fbp': 'fb.1.1724482637167.371562562162291721',
    '_gcl_au': '1.1.1462530807.1724482637',
    '_hjSessionUser_5078605': 'eyJpZCI6IjllMGNmMjdhLTYxODAtNTJhNi04NzNlLTFjMzY1YzM5MTA3MiIsImNyZWF0ZWQiOjE3MjQ0ODI2MzY2OTEsImV4aXN0aW5nIjp0cnVlfQ==',
    '_ga_06LCSL25PK': 'GS1.1.1724518865.2.0.1724518866.0.0.0',
    'at_check': 'true',
    'mbox': 'PC#9e4373a908664a9995202c0d11ad657e.41_0#1787727437|session#fbbb1e09a9c04836b8934f9f360dbdfb#1724778396',
    'AMCVS_C68C337B54EA1B460A4C98A1%40AdobeOrg': '1',
    'AMCV_C68C337B54EA1B460A4C98A1%40AdobeOrg': '179643557%7CMCIDTS%7C19962%7CMCMID%7C64161247613007455414027750273016907990%7CMCAAMLH-1725381335%7C12%7CMCAAMB-1725381335%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724783735s%7CNONE%7CvVersion%7C5.5.0',
    'gpv': 'mf%3Afundsandperformance%3Apages%3Ainav',
    's_nr': '1724776535624-Repeat',
    's_cc': 'true',
    '_hjSession_5078605': 'eyJpZCI6Ijg1MjVmN2Q3LTJkMTYtNGNiYi1iNmY3LTFjNTU0ZWVkODk2YiIsImMiOjE3MjQ3NzY1MzU4MDQsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=',
    '_ga_9LDNS8Y4ZW': 'GS1.1.1724776535.7.0.1724776535.0.0.0',
    '_ga_Z5N4HF2573': 'GS1.1.1724776535.7.0.1724776535.0.0.0',
    'ASP.NET_SessionId': '5iteg2lpoh3choqpom4p4mis',
    'NIMF': 'rd7o00000000000000000000ffff0a290762o80',
    'TS01f4aefd': '0176bf02ac82bd865db9d039fcf5e8769a4318e1082f41ca08b34e8f4e50af186b4dab6853924c72ec1958f8b61e3cd75966bff0c601116ab4e45f7607cab8ce3d1fd892f1a9dc8351ee8758b965e486854fb5de7d',
    's_ppvl': 'mf%253Afundsandperformance%253Apages%253Ainav%2C29%2C29%2C695%2C1536%2C695%2C1536%2C864%2C1.25%2CP',
    '_uetsid': '650d2a60649211efae14f1f9c170f33d',
    '_uetvid': '19c06fd061e611efb61269980bac760a',
    '_gid': 'GA1.2.736307753.1724776540',
    '_gat_gtag_UA_9474483_24': '1',
    '_ga_NNCDXFQMC2': 'GS1.1.1724776540.7.0.1724776540.60.0.0',
    '_ga': 'GA1.1.1417068970.1724482637',
    's_ppv': 'mf%253Afundsandperformance%253Apages%253Ainav%2C29%2C29%2C695%2C1536%2C395%2C1536%2C864%2C1.25%2CP',
    'TSe2513c34027': '08d45de36dab2000225ccc7281ce4bd4e3b3ba30974bef2d9d58f3b9a7412f53519eb712f088ed45080b9ed8dd11300009c8b587f1d5c965afe57f7481eac7143d92883dc72bbf5a88eacdedea12a6e4257f098ffa6dc7b62537b29c67d51704',
}

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    # 'Content-Length': '0',
    'Content-Type': 'application/json',
    # 'Cookie': '_fbp=fb.1.1724482637167.371562562162291721; _gcl_au=1.1.1462530807.1724482637; _hjSessionUser_5078605=eyJpZCI6IjllMGNmMjdhLTYxODAtNTJhNi04NzNlLTFjMzY1YzM5MTA3MiIsImNyZWF0ZWQiOjE3MjQ0ODI2MzY2OTEsImV4aXN0aW5nIjp0cnVlfQ==; _ga_06LCSL25PK=GS1.1.1724518865.2.0.1724518866.0.0.0; at_check=true; mbox=PC#9e4373a908664a9995202c0d11ad657e.41_0#1787727437|session#fbbb1e09a9c04836b8934f9f360dbdfb#1724778396; AMCVS_C68C337B54EA1B460A4C98A1%40AdobeOrg=1; AMCV_C68C337B54EA1B460A4C98A1%40AdobeOrg=179643557%7CMCIDTS%7C19962%7CMCMID%7C64161247613007455414027750273016907990%7CMCAAMLH-1725381335%7C12%7CMCAAMB-1725381335%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1724783735s%7CNONE%7CvVersion%7C5.5.0; gpv=mf%3Afundsandperformance%3Apages%3Ainav; s_nr=1724776535624-Repeat; s_cc=true; _hjSession_5078605=eyJpZCI6Ijg1MjVmN2Q3LTJkMTYtNGNiYi1iNmY3LTFjNTU0ZWVkODk2YiIsImMiOjE3MjQ3NzY1MzU4MDQsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _ga_9LDNS8Y4ZW=GS1.1.1724776535.7.0.1724776535.0.0.0; _ga_Z5N4HF2573=GS1.1.1724776535.7.0.1724776535.0.0.0; ASP.NET_SessionId=5iteg2lpoh3choqpom4p4mis; NIMF=rd7o00000000000000000000ffff0a290762o80; TS01f4aefd=0176bf02ac82bd865db9d039fcf5e8769a4318e1082f41ca08b34e8f4e50af186b4dab6853924c72ec1958f8b61e3cd75966bff0c601116ab4e45f7607cab8ce3d1fd892f1a9dc8351ee8758b965e486854fb5de7d; s_ppvl=mf%253Afundsandperformance%253Apages%253Ainav%2C29%2C29%2C695%2C1536%2C695%2C1536%2C864%2C1.25%2CP; _uetsid=650d2a60649211efae14f1f9c170f33d; _uetvid=19c06fd061e611efb61269980bac760a; _gid=GA1.2.736307753.1724776540; _gat_gtag_UA_9474483_24=1; _ga_NNCDXFQMC2=GS1.1.1724776540.7.0.1724776540.60.0.0; _ga=GA1.1.1417068970.1724482637; s_ppv=mf%253Afundsandperformance%253Apages%253Ainav%2C29%2C29%2C695%2C1536%2C395%2C1536%2C864%2C1.25%2CP; TSe2513c34027=08d45de36dab2000225ccc7281ce4bd4e3b3ba30974bef2d9d58f3b9a7412f53519eb712f088ed45080b9ed8dd11300009c8b587f1d5c965afe57f7481eac7143d92883dc72bbf5a88eacdedea12a6e4257f098ffa6dc7b62537b29c67d51704',
    'Origin': 'https://investeasy.nipponindiaim.com',
    'Pragma': 'no-cache',
    'Referer': 'https://investeasy.nipponindiaim.com/online/realtime/nav',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
# Define the start and end times

start_time = time(X, 48, 0)  # 09:00 AM
end_time = time(22, 55, 0)  # 05:00 PM

# Loop to run the code every minute within the time frame
while True:
    current_time = datetime.now().time()

    if start_time <= current_time <= end_time:
        try:
            # Sending the request
            response = requests.post('https://investeasy.nipponindiaim.com/Online/Realtime/DetailsFill',
                                     cookies=cookies, headers=headers)
            page = response.json()

            # Extracting the data
            extracted_data = [
                {
                    'Date': datetime.now().date(),  # Adding a date column
                    'Time': datetime.now().strftime('%H:%M:%S'),  # Adding a time column
                    'SchName': item['SchName'],
                    'CNav': item['CNav'],
                    'PNav': item['PNav'],
                    'NCvalue': item['NCvalue'],
                    'PChange': item['PChange'],
                    'Link': item['Link'],
                    'Realdt': item['Realdt'],
                    'Category': item['Category']

                }
                for item in page['RVDetailsList']
            ]

            # Converting the data to a DataFrame
            df = pd.DataFrame(extracted_data)

            # Saving the data to a CSV file (appending with a timestamp)
            filename = f"nipponindia_data_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename, mode='a', index=False, header=not os.path.isfile(filename))

            # Log the successful data save
            logging.info(f"Data successfully saved to {filename}")

        except Exception as e:
            # Log any errors that occur
            logging.error(f"An error occurred: {str(e)}")

    # Wait for 1 minute before the next iteration
    tm.sleep(60)
