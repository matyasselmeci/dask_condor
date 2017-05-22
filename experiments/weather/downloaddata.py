#!/usr/bin/env python

import requests

#sites = ['MSN', 'MKE', 'GRB', 'EAU', 'LSE', 'LNR', 'VOK', 'JVL', 'MTW', 'CWA', 'OSH', 'RHI', 'CMY', 'AUW']
sites = ['MSN']
year_start = 2007
year_end = 2016
url = 'http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={0}&data=tmpc&data=dwpc&data=relh&data=drct&data=sknt&data=mslp&data=p01m&year1={1}&month1=1&day1=1&year2={2}&month2=12&day2=31&tz=Etc%2FUTC&format=comma&latlon=no&direct=no&report_type=1&report_type=2'
csvfile = "{0}-{1}-to-{2}.csv"
# format with (site, year_start, year_end)

for site in sites:
    r = requests.get(url.format(site, year_start, year_end))
    with open(csvfile.format(site, year_start, year_end), 'wb') as outfile:
        for chunk in r.iter_content(chunk_size=128):
            outfile.write(chunk)

