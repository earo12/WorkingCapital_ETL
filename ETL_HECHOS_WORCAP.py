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

### Consulta de la collection Campaign 
df_campaign = pd.DataFrame(list(db.Campaign.aggregate([{'$match': {'creationDate': {'$gt': datetime(2023, 3, 8)}}},
                                                       {"$project":{  "_id":1,
                                                                      "cd_campana" : "$idCampaign",
                                                                      "nu_plazo_minimo": "$minTerm",
                                                                      "nu_plazo_maximo" : "$maxTerm",
                                                                      "nu_tasa": "$rate",  
                                                                      "tp_canal" : "$apiName",
                                                                      "tp_status_campana" : "$status",
                                                                      "fh_apertura_campaña" : "$start_campaign",
                                                                      "fh_vigencia_campaña": "$end_campaign",  
                                                                      "cd_rfc" : "$idClient",
                                                                      "fh_creacion": "$creationLocale",  
                                                                      "fh_actualizacion" : "$updatedLocal",
                                                                      "fh_deposito" : '$dispersionDate',
                                                                      "fh_envio" : '$sendDate',
                                                                      "options": "$options",  
                                                                      "cd_folio_dispersion" : "$folioDispersion",
                                                                      "optionChosen" : "$optionChosen", 
                                                                    
                                                                         }
                                                           
                                                           }])))
df_campaign['_id'] = df_campaign['_id'].astype(str) 

## JOIN CON EL STATUS DE LA CAMPAÑA

#######################################
# AGREGAMOS EL STATUS DE CADA CAMPAÑA #
#######################################
df_analitics =   pd.DataFrame(list(db.Analitics.find({})))
df_analitics =  df_analitics[['idCampaign','idClient','status']]
df_analitics =  df_analitics.rename(columns={'status': 'tp_status_oferta'})

df_campaign = df_analitics.merge(df_campaign, how='inner', left_on='idCampaign',right_on='_id' )


def select_option(row):
  """
    FUNCION QUE RECIBE LA TABLA DE AMORTIZACION Y  EXTRAE EL PAGO DIARIO Y MONTO ESCOGIDO
    
  """
  
  if row['tp_status_campana'] ==  "ACEPTED" or row['tp_status_campana']  == 'ACCEPTED' or  row['tp_status_campana'] == 'FINISHED' or row['tp_status_campana'] == 'CANCEL' or  row['tp_status_campana'] == 'CANCELLED':
    if pd.isnull(row['options']) or pd.isnull(row['optionChosen']):
      return pd.Series([0,0])
    else :
      # obetengo la columna options
      options = row['options']
      # obtengo la opci
      option_choosen = "{}".format(row['optionChosen'])
      json_option = options[option_choosen]['1']
      return pd.Series([json_option['pago'], json_option['pagoCapital'] + json_option['montoRestante']])
    
  return pd.Series([0,0])

def to_data_frame(dict):
  keys =  list(dict.keys())
  
  json_array = []
  for i in range(0,len(keys)-1):
    value = keys[i]
    json_array.append(dict[keys[i]])
    
  return pd.DataFrame(json_array)


def monto_total_credito(row):
  if row['tp_status_campana'] ==  "ACEPTED" or row['tp_status_campana']  == 'ACCEPTED' or row['tp_status_campana'] == 'FINISHED' or  row['tp_status_campana'] == 'CANCEL' or row['tp_status_campana'] == 'CANCELLED':
    if pd.isnull(row['options']) or pd.isnull(row['optionChosen']):
      return pd.Series([0, 0])
    else :
      json_anortizacion = row['options'][str(row['optionChosen'])]

      df_amortizacion = to_data_frame(json_anortizacion)

      return pd.Series([sum(df_amortizacion['iva']) , sum(df_amortizacion['pagoInteres']) ])
    
  return pd.Series([0, 0])