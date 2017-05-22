#!/usr/bin/env python

import requests
import time

def download(site, year_start, year_end):
    url = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
    params = { 'station': site
             , 'data': ['tmpc', 'dwpc', 'relh', 'drct', 'sknt', 'mspl', 'p01m']
             , 'year1': year_start
             , 'month1': 1
             , 'day1': 1
             , 'year2': year_end
             , 'month2': 12
             , 'day2': 31
             , 'tz': 'Etc/UTC'
             , 'format': 'comma'
             , 'latlon': 'no'
             , 'report_type': [1, 2]
    }

    return requests.get(url, params)

sites = ['MSN', 'MKE', 'GRB', 'EAU', 'LSE', 'LNR', 'VOK', 'JVL', 'MTW', 'CWA', 'OSH', 'RHI', 'CMY', 'AUW']
year_start = 2007
year_end = 2016
csvfile = "{0}-{1}-to-{2}.csv"
# format with (site, year_start, year_end)

for site in sites:
    r = download(site, year_start, year_end)
    with open(csvfile.format(site, year_start, year_end), 'wb') as outfile:
        for chunk in r.iter_content(chunk_size=128):
            outfile.write(chunk)
    time.sleep(5)
