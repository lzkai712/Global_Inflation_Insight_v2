from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('./durable-height-412117-2cfee8131ab0.json')

project_id = 'durable-height-412117'
client = bigquery.Client(credentials= credentials,project=project_id)

'''query = client.query("""
   SELECT * FROM `durable-height-412117.global_inflation.Energy` LIMIT 1000
""")

results = query.result()
for row in results:
   print(row)'''