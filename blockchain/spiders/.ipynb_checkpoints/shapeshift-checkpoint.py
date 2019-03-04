import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from blockchain.spiders.parse_tx import satoshi_to_btc, insert_tx
from scrapy.conf import settings

class ShapeshiftSpider(scrapy.Spider):
    name = "shapeshift"

    def start_requests(self):
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['SHAPESHIFT_DB']]
        urls = list(selfdb.ss_url.find({'retrieved': False}))
        print('NUM URLS TO SCRAPE', len(urls))
        for url in urls:
            yield scrapy.Request(url=url['url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        orig_obj = response.meta['obj']
        obj = json.loads(response.body)
        obj['retrieved'] = datetime.now()
        if 'tag' in orig_obj.keys():
            obj['tag'] = orig_obj['tag']
        try:
            self.db.ss_resp.insert_many([obj])
        except:
            pass
        orig_obj['retrieved'] = True
        orig_obj['last_scraped'] = datetime.now()
        self.db.ss_url.replace_one({'_id': ObjectId(orig_obj['_id'])}, orig_obj, upsert=True)