import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from bs4 import BeautifulSoup

client = MongoClient('mongodb://localhost:27017')
db = client.btc


class WalletExplorerAddrScraper(scrapy.Spider):
    name = "wallet_explorer_addr"

    def start_requests(self):
        urls = list(db.wallet_explorer_exchange.find({}))
        print(len(urls))
        for url in urls:
            print(url['wallet_url'])
            yield scrapy.Request(url=url['wallet_url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        orig_obj = response.meta['obj']
        header_lookup = {
            0: 'wallet_address',
            1: 'btc',
            2: 'num_incoming_txs',
            3: 'last_used_block'  
        }
        actions = {
            0: lambda x: x.find('a').text,
            1: lambda x: float(x.text),
            2: lambda x: int(x.text),
            3: lambda x: x.text
        }
        soup = BeautifulSoup(response.body, 'html.parser')
        for row in soup.find('table').find_all('tr'):
            obj = {}
            for idx, col in enumerate(row.find_all('td')):
                obj[header_lookup[idx]] = actions[idx](col)
            obj['dt'] = datetime.now()
            obj['exchange_name'] = orig_obj['exchange_name']
            try:
                db.wallet_explorer_addr.insert_many([obj])
            except:
                pass
        
        paging = soup.find('div', class_='paging')
        if paging is not None:
            paging_links = list(filter(lambda x: 'next' in x.text.lower(), paging.find_all('a')))
            if len(paging_links) > 0:
                yield response.follow(paging_links[0]['href'], callback=self.parse, meta={'obj': orig_obj})
                
                    
                