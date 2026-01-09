import os
SEC_API_KEY = os.environ.get("SEC_API_KEY", 
                             "INSERT API KEY HERE")
SEC_BASE_URL = "https://api.sec-api.io"


STOCKDATA_API_KEY = os.environ.get("STOCKDATA_API_KEY", 
                                   "INSERT API KEY HERE")
STOCKDATA_BASE_URL = "https://api.stockdata.org/v1"