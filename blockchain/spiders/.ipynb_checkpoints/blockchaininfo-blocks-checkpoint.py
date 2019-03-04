import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from blockchain.spiders.parse_tx import satoshi_to_btc, insert_tx

client = MongoClient('mongodb://localhost:27017')
db = client.btc


class BlockSpider(scrapy.Spider):
    name = "blockchaininfo_blocks"

    def start_requests(self):
        print('NUM URLS TO SCRAPE', db.block_pointers_url.find({'retrieved': False}).count())
        for url in list(db.block_pointers_url.find({'retrieved': False})):
            yield scrapy.Request(url=url['url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        blocks = json.loads(response.body)
        orig_obj = response.meta['obj']
        
        for obj in blocks['blocks']:
            obj['dt'] = datetime.fromtimestamp(obj['time'])
            obj['retrieved_details'] = False
            try:
                db.block_pointers.insert_many([obj])
            except:
                pass

        orig_obj['retrieved'] = True
        orig_obj['last_scraped'] = datetime.now()
        db.block_pointers_url.replace_one({'_id': ObjectId(orig_obj['_id'])}, orig_obj, upsert=True)