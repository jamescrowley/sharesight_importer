import os
import argparse
from time import sleep
import requests
import json

def import_rates(cookie_value, dates_file="dates.txt"):
    # API endpoint template
    url_template = "https://portfolio.sharesight.com/api/v3.0-internal/exchange_rates.json?date={date}&codes[]=GBP&codes[]=AUD&codes[]=EUR&codes[]=USD&codes[]=JPY&show_all_crosses=false"

    # Custom headers
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,uk;q=0.7',
        'cache-control': 'no-cache',
        'dnt': '1',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Microsoft Edge";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0',
        'cookie': cookie_value
    }

    # Read dates from file
    with open(dates_file, "r") as f:
        dates = [line.strip() for line in f if line.strip()]

    # Loop through each date
    for date in dates:
        filename = f"rates/{date}.json"
        if os.path.exists(filename):
            print(f"File {filename} already exists, skipping.")
            continue
        url = url_template.format(date=date)
        response = requests.get(url, headers=headers)
        sleep(0.1)

        if response.status_code == 200:
            with open(filename, "w") as outfile:
                json.dump(response.json(), outfile, indent=2)
            print(f"Saved {date}.json")
        else:
            print(f"Failed for {date}: {response.status_code}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import exchange rates from Sharesight API')
    parser.add_argument('cookie', help='Cookie string for authentication')
    parser.add_argument('-d', '--dates-file', default='dates.txt', help='File containing dates to fetch (default: dates.txt)')
    
    args = parser.parse_args()
    import_rates(args.cookie, args.dates_file)