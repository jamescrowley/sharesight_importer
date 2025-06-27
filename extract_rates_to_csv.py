import json
import os
import csv
from collections import defaultdict

def extract_rates_to_csv():
    all_rates = defaultdict(dict)
    all_pairs = set()
    
    for filename in os.listdir('rates'):
        if not filename.endswith('.json'):
            continue
            
        date_str = filename[:-5]  # Remove .json extension
        
        with open(f'rates/{filename}', 'r') as f:
            data = json.load(f)
        
        for pair, rate_info in data.get('exchange_rates', {}).items():
            all_pairs.add(pair)
            all_rates[date_str][pair] = rate_info['rate']
    
    with open('exchange_rates_ss.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['date'] + sorted(all_pairs))
        writer.writeheader()
        
        for date in sorted(all_rates.keys()):
            row = {'date': date}
            row.update({pair: all_rates[date].get(pair, '') for pair in sorted(all_pairs)})
            writer.writerow(row)
    
    print(f"Extracted {len(all_rates)} dates and {len(all_pairs)} currency pairs")

if __name__ == "__main__":
    extract_rates_to_csv()