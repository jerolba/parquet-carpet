#
# Copyright 2023 Jerónimo López Bezanilla
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

data_nested_record = [
    {
        'id': '001',
        'company': 'XYZ ltd',
        'location': 'London',
        'info': {
            'a' : 10,
            'b' : "hello"
        }
    },
    {
        'id': '002',
        'company': 'PQR Associates',
        'location': 'Abu Dhabi',
        'info': {
            'a' : 12,
            'b' : "bye"
        }
    }
]

df_nested_struct = pd.json_normalize(data_nested_record, max_level=0)

print("NESTED RECORD NORMALIZED")
print(df_nested_struct)
df_nested_struct.to_parquet("/home/data/python_nested_record.parquet")

data_nested_collecton = [
    {
        'id': '001',
        'company': 'XYZ pvt ltd',
        'location': 'London',
        'info': [{
            'a' : 10,
            'b' : "hello"
        },{
            'a' : 20,
            'b' : "hi"
        }]
    },
    {
        'id': '002',
        'company': 'PQR Associates',
        'location': 'Abu Dhabi',
        'info': [{
            'a' : 12,
            'b' : "bye"
        }]
    }
]

df_nested_collection_struct = pd.json_normalize(data_nested_collecton)
print("NESTED COLLECTION")
print(df_nested_collection_struct)
df_nested_collection_struct.to_parquet("/home/data/python_nested_collection.parquet", use_compliant_nested_type=False)
print("NESTED COLLECTION COMPLIANT")
df_nested_collection_struct.to_parquet("/home/data/python_nested_collection_compliant.parquet", use_compliant_nested_type=True)
