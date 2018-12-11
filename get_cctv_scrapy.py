# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 11:27:49 2018

@author: Alan.Toppen
"""

import scrapy
from scrapy.crawler import CrawlerProcess
import requests
import re
import json
from datetime import datetime
import pytz
import os


class CamerasSpider(scrapy.Spider):
    name = "cameras"

    def start_requests(self):
        urls = [
            'http://www.511ga.org/mobile/?action=view_list_cameras&run_mode=cameras&template=lite&trail=main_menu',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_)

    def parse_(self, response):
        
        gdot_folder = os.path.join(os.path.expanduser('~'), 'Code', 'GDOT')
        base_path = os.path.join(gdot_folder, 'cctvlogs')
        if not os.path.exists(base_path):
            os.mkdir(base_path)
        
        for link in response.xpath('//@href').extract():

            url = response.follow(link).url
            print('---')
            print(url)
            print('---')

            if 'jpg' in url:
                resp = requests.get(url, timeout=5, proxies={'http': 'http://GADOT\V0010894:11400Comm20191@gdot-enterprise:8080'})
                
                dict_ = dict(resp.headers)
                dict_.update({'ID': re.search('(?<=/cameras/).*(?=\.jpg)', url).group(0)})
                json_data = json.dumps(dict_)
                print(dict_['ID'])
				
                resp.close()
                
                if 'Last-Modified' in dict_.keys():
                    dt = datetime.strptime(dict_['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
                    dt_ = dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
                    fn = os.path.join(base_path, 'cctvlog_{}.json'.format(dt_.strftime('%Y-%m-%d')))
                    
                    with open(fn, 'a') as f:
                        f.write(json_data + '\n')
                
                    yield dict_
            
            elif '?action=main_menu' in url:
                pass

            elif 'idents=cctv' in url: #elif '511ga' in url:
                yield scrapy.Request(url, callback=self.parse_)
            else:
                pass
            

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(CamerasSpider)
process.start() # the script will block here until the crawling is finished