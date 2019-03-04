import scrapy
import json
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from scrapy.conf import settings

class EtherscanSpider(scrapy.Spider):
    name = "etherscan"

    def start_requests(self):
        connection = MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['ETH_DB']]
        
        urls_to_scrape = list(self.db.etherscan_url.find({'retrieved': False}))
        print('NUM URLS TO SCRAPE', len(urls_to_scrape))
        for url in urls_to_scrape:
            yield scrapy.Request(url=url['url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        data = json.loads(response.body)
        orig_obj = response.meta['obj']
        if data['message'] == 'OK':
            for obj in data['result']:
                obj['value'] = float(obj['value'])
                obj['eth'] = obj['value']/1000000000000000000.0
                obj['timeStamp'] = int(obj['timeStamp'])
                obj['dt'] = datetime.fromtimestamp(obj['timeStamp'])
                obj['dt_str'] = obj['dt'].strftime('%m%d%y')
                if 'tag' in orig_obj.keys():
                    obj['tag'] = orig_obj['tag']
                if 'account_addr' in orig_obj.keys():
                    obj['account_addr'] = orig_obj['account_addr']                
                try:
                    self.db.etherscan_tx.insert_many([obj])
                except:
                    pass
        
        
        if data['message'] == 'OK':
            orig_obj['has_result'] = True
            orig_obj['retrieved'] = True
        orig_obj['last_scraped'] = datetime.now()
        self.db.etherscan_url.replace_one({'_id': ObjectId(orig_obj['_id'])}, orig_obj, upsert=True)