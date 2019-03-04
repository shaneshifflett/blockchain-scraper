import time
import pytz
import scrapy
import json
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
tz = pytz.timezone('US/Eastern')
client = MongoClient('mongodb://localhost:27017')
db = client.coinmarketcap


class PricingSpider(scrapy.Spider):
    name = "pricing_coinmarketcap"

    def start_requests(self):
        urls_to_scrape = list(db.pricing_url.find({'retrieved': False}))
        print('NUM URLS TO SCRAPE', len(urls_to_scrape))
        for url in urls_to_scrape:
            yield scrapy.Request(url=url['url'], callback=self.parse, meta={'obj': url})

    def parse(self, response):
        orig_obj = response.meta['obj']
        data = json.loads(response.body)
        market_cap = list(map(lambda x: [datetime.fromtimestamp(x[0]/1000, tz), x[1]], data['market_cap_by_available_supply']))
        price_usd = list(map(lambda x: [datetime.fromtimestamp(x[0]/1000, tz), x[1]], data['price_usd']))
        price_btc = list(map(lambda x: [datetime.fromtimestamp(x[0]/1000, tz), x[1]], data['price_btc']))
        volume_usd = list(map(lambda x: [datetime.fromtimestamp(x[0]/1000, tz), x[1]], data['volume_usd']))  
        
        for idx, row in enumerate(price_usd):
            obj = {
                'amount': row[1],
                'timestamp': row[0],
                'coinmarketcap_slug': orig_obj['slug'],
                'date_str': row[0].strftime("%m-%d-%Y"),
                'market_cap': market_cap[idx][1],
                'volume': volume_usd[idx][1],
                'symbol': orig_obj['symbol'],
                'currency': 'usd'
            }
            try:

                #db.pricing.insert_one(obj)
                db.pricing.replace_one({'coinmarketcap_slug': orig_obj['slug'], 'timestamp': row[0]}, obj, upsert=True)
            except Exception as e:
                pass

        
        orig_obj['has_result'] = True
        orig_obj['retrieved'] = True
        orig_obj['last_scraped'] = datetime.now()
        db.pricing_url.replace_one({'_id': ObjectId(orig_obj['_id'])}, orig_obj, upsert=True)