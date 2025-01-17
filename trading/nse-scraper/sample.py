import requests
import pandas as pd

# def fetch_data_from_api(params):
#     response = requests.get('https://www.nseindia.com/all-reports', params=params)
#     if response.status_code == 200:
#         return pd.DataFrame(response.json())  # Adjust based on API's response format
#     else:
#         raise Exception("API call failed with status code:", response.status_code)
    
# print(api_data[['ROC_RS_7', 'ROC_RS_14']])

def fetch_data_from_api(date='2025-01-14'):
    url = f"http://example-api.com/data?date={date}"
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json())  # Adjust based on your API's response format
    else:
        raise Exception(f"API call failed: {response.status_code}")
fetch_data_from_api(date='2025-01-14') 



