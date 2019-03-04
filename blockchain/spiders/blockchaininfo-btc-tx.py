import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from blockchain.spiders.parse_tx import satoshi_to_btc, insert_tx, insert_tx_raw

client = MongoClient('mongodb://localhost:27017')
db = client.btc


class IllicitSpider(scrapy.Spider):
    name = "blockchaininfo_btc_tx"

    def start_requests(self):
        print('NUM URLS TO SCRAPE', db.search_url.find({'retrieved': False}).count())
        for url in list(db.search_url.find({'retrieved': False})):
            print(url['url'])
            yield scrapy.Request(url=url['url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        obj = json.loads(response.body)
        orig_obj = response.meta['obj']
        transactions = obj['txs']
        
        obj['total_btc_received'] = obj['total_received'] / satoshi_to_btc
        obj['total_btc_sent'] = obj['total_sent'] / satoshi_to_btc
        obj['final_btc_balance'] = obj['final_balance'] / satoshi_to_btc
        obj['n_tx_downloaded'] = len(transactions)
        obj['last_updated'] = datetime.now()
        for tx in transactions:
            if 'search_address' in orig_obj.keys():
                insert_tx(obj['address'], orig_obj['search_address'], orig_obj['hop_num'], tx)
            else:
                insert_tx_raw(tx)

        del obj['txs']
        try:
            db.wallet_summary.insert_many([obj])
        except:
            pass
        
        orig_obj['retrieved'] = True
        orig_obj['last_scraped'] = datetime.now()
        db.search_url.replace_one({'_id': ObjectId(orig_obj['_id'])}, orig_obj, upsert=True)