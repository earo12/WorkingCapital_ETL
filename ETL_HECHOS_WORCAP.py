# Databricks notebook source
# pip install pymongo

# COMMAND ----------

import csv
import pytz
import random
import pandas as pd
import random
import urllib.parse
import json
from pymongo import MongoClient
from connections_mit import Secrets, AES, Redshift, S3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json 


# COMMAND ----------

import csv
import pytz
import pandas as pd
import numpy as np
import urllib.parse

from pymongo import MongoClient
from connections_mit import Redshift
from connections_mit import Secrets, AES
from datetime import datetime, timedelta

import random
import urllib.parse

from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid 


import json
from base64 import b64encode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from connections_mit import Secrets 
import base64
import binascii
from Crypto import Random
import os

#################################
# FUNCIÓN PARA DECIFRAR  EN GCM #
#################################

def decrypt_gcm(data, master_key):
  key = binascii.unhexlify(master_key.encode('utf-8'))
  data = base64.b64decode(data.encode('utf-8'))
  nonce, tag = data[:12], data[-16:]
  cipher = AES.new(key, AES.MODE_GCM, nonce)
  decrypt_value = cipher.decrypt_and_verify(data[12:-16], tag)
  return decrypt_value.decode('utf-8')

######################################################################
# CREDENCIALES Y CONEXIÓN A LA BASE DE DATOS DE WORKING CAPITAL PROD #
######################################################################

secret = Secrets.get_secret('worcap_analytics_prod')
user = decrypt_gcm(secret['username'], secret['llave de cifrado'])
password = decrypt_gcm(secret['password'], secret['llave de cifrado'])

db = secret['dbname']
host = secret['host']

mongo_uri = "mongodb+srv://" + urllib.parse.quote_plus(user) + ":" + password + "@" + host + "/" + db  + "?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"

client = MongoClient(mongo_uri)
db = client[db]
