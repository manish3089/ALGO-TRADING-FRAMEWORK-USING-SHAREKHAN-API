from SharekhanApi.sharekhanConnect import SharekhanConnect

api_key = "GIaiFf8eFa1gOfdC2W4m6RUb9YOvVYIu"
with open('access.txt', 'r') as file:
    access_token = file.read().strip()

sharekhan = SharekhanConnect(api_key, access_token = access_token)
    
orderparams = {
        "customerId": 4036182,
        "scripCode": 463393,
        "tradingSymbol": "GOLDM",
        "exchange": "MX",
        "transactionType": "B",
        "quantity": 100,
        "triggerPrice": "0",
        "price": "0",
        "rmsCode": "ANY",
        "afterHour": "N",
        "orderType": "NORMAL",
        "expiry": "03/10/2025",
        "instrumentType": "FS",
        "optionType": "XX",
        "strikePrice": "-1",
        "channelUser": "4036182",
        "validity": "GFD",
        "requestType": "NEW",
        "productType": "INVESTMENT"
      }
try:
    order = sharekhan.placeOrder(orderparams)
    print(f"✅ PlaceOrder Response: {order}")
except Exception as e:
    print(f"⚠️ Order placement failed: {e}")