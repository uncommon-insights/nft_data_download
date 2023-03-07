import requests
import time
from pymongo import MongoClient

# solchicks pump till 28 feb 2022, so we take +3 months which is ~ 31 may 2022
# note we are getting data from beginning to compute balances from mints minus sale transactions
metadata = {"SolChicks": {'filter': {"firstVerifiedCreator": ['9nicnEJ1pfssv3CWxQFMebFBrs5Cqun8WEmwwLqapLeX']}},
            "DeGods": {'filter': {"verifiedCollectionAddress": ["6XxjKYFbcndh2gDcsUrmZgVEsoDxXMnfsaGY6fpTJzNr"]}}}

helius_key_fname = './private/helius.txt'
helius_raw_endpoint = "https://api.helius.xyz/v1/nft-events?api-key="
limit = 1000
rate_limit_sec = 6  # ratelimit: 10/min so 1 every 6 seconds. total 1k API calls allowed
endTime = 1653926400
errors = []

previous_time = time.time()
f = open(helius_key_fname, 'r')
helius_api_key = f.read()
f.close()
helius_endpoint = helius_raw_endpoint + helius_api_key

CONNECTION_STRING = "mongodb://localhost:27017"
client = MongoClient(CONNECTION_STRING)
dbname = client['nft_data']


def handle_rate_limit(prev_time):
    curr_time = time.time()
    delta_time = curr_time - prev_time
    if delta_time < rate_limit_sec:
        time.sleep(rate_limit_sec - delta_time)
        return time.time()
    else:
        return curr_time


def download_batch(coll_name, nft_query, prev_time):
    prev_time = handle_rate_limit(prev_time)
    response = requests.request("POST", helius_endpoint, json=nft_query)
    data = response.json()
    if response.status_code == 200:
        if data['result']:
            coll_name.insert_many(data['result'])
            ts, desc = data['result'][-1]['timestamp'], data['result'][-1]['description']
            print('timestamp ', ts)
            print(desc)
        else:
            print('no more data left')
    else:
        print(data['error'])
        errors.append(data['error'])
    if 'paginationToken' not in data:
        data['paginationToken'] = ''
    return prev_time, data['paginationToken']


for collection in metadata:
    collection_name = dbname[collection]
    nft_filter = metadata[collection]['filter']
    query = {"query": {"types": ["NFT_SALE", "NFT_MINT"], "endTime": endTime, "nftCollectionFilters": nft_filter},
             "options": {"limit": limit}}
    print('first query for ', collection)
    previous_time, paginationToken = download_batch(collection_name, query, previous_time)
    while paginationToken:
        query['options']['paginationToken'] = paginationToken
        previous_time, paginationToken = download_batch(collection_name, query, previous_time)

client.close()

print("download finished")
if errors:
    print('errors found:')
    for error_msg in errors:
        print(error_msg)




