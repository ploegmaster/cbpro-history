import pandas as pd
import numpy as np
import gdax
import os
from datetime import datetime, timedelta
from time import sleep


#
# read all about the API on: https://docs.pro.coinbase.com/
#


# 
# setup configuration
#
product = 'BTC-EUR'
granularity = 60*60 #seconds
start = (datetime.now() - timedelta(days=200))
end = datetime.now()
delay = 1
output_file = datetime.now().strftime("%Y%m%d_%H%M%S ") + '_data200.txt'

#
# end of setup configuration
#

if os.path.exists(output_file):
    os.remove(output_file)
    
#######
## script
#######

public_client = gdax.PublicClient()
data_frames = []

# GDAX allows 200 max per retrieve
step = timedelta(seconds=(granularity * 200))

periods = (end - start).total_seconds() / granularity
period_start = start
period_end = start + step

if period_end > end:
    period_end = end

while period_end <= end:
    # Retrieve the set
    records = public_client.get_product_historic_rates(product, granularity=granularity, start=period_start.isoformat(), end=period_end.isoformat())
    if not isinstance(records, (list)):
        raise TypeError("Instance is not a list: %s" % records)

    # coinbasepro has a strict use policy
    if delay and delay > 0:
        sleep(delay)

    # Iterate and observe any remaining elements after boundary
    period_start += step
    period_end += step
    if period_end > end and period_start < end:
        period_end = end

    if not records:
        continue

    # Convert to Pandas DataFrame
    records = np.array(records)
    df = pd.DataFrame(records[:,1:], index=records[:,0], columns=['Low', 'High', 'Open', 'Close', 'Volume'])
    df.index = pd.to_datetime(pd.to_numeric(df.index), utc=True, unit='s')
    df = df.sort_index(ascending=True)

    print ("exporting " + str(period_start))
    df.to_csv(output_file, mode='a+', sep='\t', header=False)

print ("done")

