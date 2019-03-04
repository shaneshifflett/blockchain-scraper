import numpy as np
from bson.int64 import Int64
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('mongodb://localhost:27017')
db = client.btc

satoshi_to_btc = 100000000

def insert_tx(wallet_addr, search_addr, hop_num, tx):
    input_addrs = list(map(lambda x: x['prev_out']['addr'] if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else [], tx['inputs'])) if 'inputs' in tx.keys() else []
    tx['total_input_value'] = np.sum(list(map(lambda x: x['prev_out']['value'] if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else 0.0, tx['inputs']))) if 'inputs' in tx.keys() else 0.0
    tx['total_input_value'] = Int64(tx['total_input_value'])
    tx['input_btc'] = tx['total_input_value'] / satoshi_to_btc
    output_addrs = list(map(lambda x: x['addr'] if 'addr' in x.keys() else [], tx['out'])) if 'out' in tx.keys() else []
    tx['total_output_value'] = np.sum(list(map(lambda x: x['value'] if 'value' in x.keys() else 0.0, tx['out']))) if 'out' in tx.keys() else 0.0
    tx['total_output_value'] = Int64(tx['total_output_value'])
    tx['output_btc'] = tx['total_output_value'] / satoshi_to_btc
    tx['num_input_addrs'] = len(input_addrs)
    tx['num_output_addrs'] = len(output_addrs)
    tx['dt'] = datetime.fromtimestamp(tx['time'])
    tx['wallet_address'] = wallet_addr
    tx['search_address'] = search_addr
    tx['hop_num'] = hop_num

    try:
        db.search_txs.insert_many([tx])
    except Exception as e:
        #print(e)
        pass

    from_txs = list(map(lambda x: {'search_address': search_addr, 'wallet_address': wallet_addr, 'hop_num': hop_num, 'addr': x['prev_out']['addr'], 'btc': x['prev_out']['value'] / satoshi_to_btc, 'hash': tx['hash'], 'dt': tx['dt'], 'dir': 'from'}  if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else None, tx['inputs'])) if 'inputs' in tx.keys() else []
    from_txs = list(filter(lambda x: x is not None, from_txs))

    if len(from_txs) > 0:
        try:
            db.search_tx_summaries.insert_many(from_txs, ordered=False)
        except Exception as e:
            #print(e)
            pass

    to_txs = list(map(lambda x: {'search_address': search_addr, 'wallet_address': wallet_addr, 'hop_num': hop_num, 'addr': x['addr'], 'btc': x['value'] / satoshi_to_btc, 'hash': tx['hash'], 'dt': tx['dt'], 'dir': 'to'} if 'addr' in x.keys() else None, tx['out'])) if 'out' in tx.keys() else []
    to_txs = list(filter(lambda x: x is not None, to_txs))

    if len(to_txs) > 0:
        try:
            db.search_tx_summaries.insert_many(to_txs, ordered=False)
        except Exception as e:
            pass

        
def insert_tx_raw(tx):
        input_addrs = list(map(lambda x: x['prev_out']['addr'] if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else [], tx['inputs'])) if 'inputs' in tx.keys() else []
        tx['total_input_value'] = np.sum(list(map(lambda x: x['prev_out']['value'] if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else 0.0, tx['inputs']))) if 'inputs' in tx.keys() else 0.0
        tx['total_input_value'] = Int64(tx['total_input_value'])
        tx['total_input_value_btc'] = tx['total_input_value'] / satoshi_to_btc
        output_addrs = list(map(lambda x: x['addr'] if 'addr' in x.keys() else [], tx['out'])) if 'out' in tx.keys() else []
        tx['total_output_value'] = np.sum(list(map(lambda x: x['value'] if 'value' in x.keys() else 0.0, tx['out']))) if 'out' in tx.keys() else 0.0
        tx['total_output_value'] = Int64(tx['total_output_value'])
        tx['total_output_value_btc'] = tx['total_output_value'] / satoshi_to_btc
        tx['num_input_addrs'] = len(input_addrs)
        tx['num_output_addrs'] = len(output_addrs)
        tx['dt'] = datetime.fromtimestamp(tx['time'])

        try:
            db.blockchaininfo_tx.insert_many([tx])
        except Exception as e:
            #print(e)
            pass

        from_txs = list(map(lambda x: {'addr': x['prev_out']['addr'], 'btc': x['prev_out']['value'] / satoshi_to_btc, 'hash': tx['hash'], 'dt': tx['dt'], 'dir': 'from'}  if 'prev_out' in x.keys() and 'addr' in x['prev_out'].keys() else None, tx['inputs'])) if 'inputs' in tx.keys() else []
        from_txs = list(filter(lambda x: x is not None, from_txs))
        
        if len(from_txs) > 0:
            try:
                db.blockchaininfo_wallet_tx.insert_many(from_txs, ordered=False)
            except Exception as e:
                #print(e)
                pass

        to_txs = list(map(lambda x: {'addr': x['addr'], 'btc': x['value'] / satoshi_to_btc, 'hash': tx['hash'], 'dt': tx['dt'], 'dir': 'to'} if 'addr' in x.keys() else None, tx['out'])) if 'out' in tx.keys() else []
        to_txs = list(filter(lambda x: x is not None, to_txs))

        if len(to_txs) > 0:
            try:
                db.blockchaininfo_wallet_tx.insert_many(to_txs, ordered=False)
            except Exception as e:
                pass