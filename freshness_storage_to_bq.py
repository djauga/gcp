from google.cloud import bigquery, storage
from collections import OrderedDict
import re
import json
import decimal

client = bigquery.Client()
storage_client = storage.Client()
project = client.project

dataset_id = "de_hw_daumantas_jauga"
table_id = "freshness_results"
gcs_bucket_name = 'kilo_de_hw_files'


def get_vals(test_dict, key_list):
   for i, j in test_dict.items():
     if i in key_list:
        yield (i, j)
     yield from [] if not isinstance(j, dict) else get_vals(j, key_list)


def delete_keys_from_dict(d, to_delete):
    if isinstance(to_delete, str):
        to_delete = [to_delete]
    if isinstance(d, dict):
        for single_to_delete in set(to_delete):
            if single_to_delete in d:
                del d[single_to_delete]
        for k, v in d.items():
            delete_keys_from_dict(v, to_delete)
    elif isinstance(d, list):
        for i in d:
            delete_keys_from_dict(i, to_delete)

def rename_dict_key(the_dict, old_key_name, new_key_name):
  if type(the_dict) in [dict, OrderedDict]:
    for key in list(the_dict.keys()):
      if key == old_key_name:
        the_dict[new_key_name] = the_dict[old_key_name]
        del the_dict[key]
    for obj in the_dict:
      rename_dict_key(the_dict[obj], old_key_name, new_key_name)
  elif type(the_dict) is list:
    for obj in the_dict:
      rename_dict_key(obj, old_key_name, new_key_name)
    


json_records = []
key_list = ["generated_at","invocation_id", 'results']
# List all JSON files in the GCS input folder and process them
json_files = [file for file in storage_client.bucket(gcs_bucket_name).list_blobs(prefix="freshness/") if file.name.endswith(".json")]

for json_blob in json_files:
    json_data = json_blob.download_as_text()
    data_dict = json.loads(json_data)
    res = dict(get_vals(data_dict, key_list))
    for result in res['results']:
        if result: 
            schema = result['unique_id']
            schema_filter = result["criteria"]["filter"]
            print(schema_filter)
            result['project_name'] = re.search('\.(.*?)\.(.*?)\.', schema).group(1)
            result['schema_name'] = re.search('\.(.*?)\.(.*?)\.', schema).group(2)
            result['table_name'] = re.search('([^\.]+$)', schema).group(1)
            result["warn_after_count"] = result['criteria']['warn_after']["count"]
            result["warn_after_period"] = result['criteria']['warn_after']["period"]
            result["error_after_count"] = result['criteria']['error_after']["count"]
            result["error_after_period"] = result['criteria']['error_after']["period"]
        elif schema_filter:
            result['filter_field'] = re.search('\,(.*)\,(.*?)', schema_filter).group(1)
            result['filter_type'] = re.search('\)([^)]*)(\d+)', schema_filter).group(1)
            result['filter_value'] = re.search('([<>]=?|==)\s*(\d+)', schema_filter).group(2)
            bytes_processed = result['adapter_response']['bytes_processed']
            result['adapter_response']['price'] = float(decimal.Decimal((bytes_processed / 1073741824) * 0.02))
        else:   
            result[['filter_field','filter_type','filter_value', "project_name"
                    ,'schema_name', 'table_name', 'warn_after_count', 'warn_after_period'
                    ,'error_after_count', 'error_after_period', 'filter_field', 'filter_type'
                    ,'filter_value']] = None
            
            
            result['adapter_response']['price'] = None
            
        bytes_processed = result['adapter_response']['bytes_processed']
        result['adapter_response']['price'] = float(decimal.Decimal((bytes_processed / 1073741824) * 0.02))
    

    delete_keys_from_dict(res, ['criteria','execution_time','unique_id','count','period','thread_id', 'rows_affected', 'code', '_message'])
    rename_dict_key(res, "invocation_id", "id")

    rename_dict_key(res, "max_loaded_at", "latest_loaded_at")
    rename_dict_key(res, "snapshotted_at", "queried_at")
    rename_dict_key(res, "max_loaded_at_time_ago_in_s", "time_since_last_row_arrived_in_s")
    rename_dict_key(res, "location", "job_location")


    json_records.append(res)



with open ("sample-json-data_v.json", "w") as jsonwrite:
   for item in json_records:
       jsonwrite.write(json.dumps(item) + '\n') #newline delimited json file

schema = [
    bigquery.SchemaField("generated_at", "TIMESTAMP"),
    bigquery.SchemaField("results", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("adapter_response", "RECORD", fields=[
            bigquery.SchemaField("bytes_processed", "INTEGER"),
            bigquery.SchemaField("bytes_billed", "INTEGER"),
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("job_id", "STRING"),
            bigquery.SchemaField("slot_ms", "INTEGER"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("job_location", "STRING"),
        ]),
        bigquery.SchemaField("timing", "RECORD",mode='REPEATED', fields=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("completed_at", "TIMESTAMP"),
        ]),
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("schema_name", "STRING"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("warn_after_count", "INTEGER"),
        bigquery.SchemaField("warn_after_period", "STRING"),
        bigquery.SchemaField("error_after_count", "INTEGER"),
        bigquery.SchemaField("error_after_period", "STRING"),
        bigquery.SchemaField("filter_field", "STRING"),
        bigquery.SchemaField("filter_type", "STRING"),        
        bigquery.SchemaField("filter_value", "FLOAT"),        
        bigquery.SchemaField("latest_loaded_at", "TIMESTAMP"),        
        bigquery.SchemaField("queried_at", "TIMESTAMP"),        
        bigquery.SchemaField("time_since_last_row_arrived_in_s", "FLOAT")   
    ]),
    bigquery.SchemaField("id", "STRING"),

]

dataset_ref = bigquery.DatasetReference(project ,'de_hw_daumantas_jauga')
table_ref = dataset_ref.table(table_id)
table = bigquery.Table(table_ref, schema=schema)

table.clustering_fields = ["id"]

table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="generated_at",   
) 
#table = client.create_table(table)      

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = schema


with open("sample-json-data_v.json", "rb") as source_file:
   job = client.load_table_from_file(
       source_file,
       table_ref,
       job_config=job_config,
   )  # API request

job.result()  # Waits for table load to complete.

