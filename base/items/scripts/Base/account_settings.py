# coding: utf-8
#!/usr/bin/python

#####
# Made by Roman Lezama
#####
from linkaform_api import settings
#prod
#settings.mongo_hosts = 'db2.linkaform.com:27017,db3.linkaform.com:27017,db4.linkaform.com:27017'
#settings.mongo_port = 27017
#preprod
settings.mongo_hosts = 'dbs2.lkf.cloud:27918'
settings.mongo_port = 27918
#local
#settings.mongo_hosts = '192.168.0.25:27017'
#settings.mongo_port = 27017

#settings.MONGODB_URI = 'mongodb://%s/'%(settings.mongo_hosts)
config = {
    'USERNAME' : 'user.name@dominio.com',
    'APIKEY': '3118bf9293a04910006b8440d8921fb404eacccc',
    'PROTOCOL' : 'https', #http or https
    'HOST' : 'preprod.linkaform.com',
    'MONGODB_PORT':settings.mongo_port,
    'MONGODB_USER': 'account_123456',
    'MONGODB_HOST': settings.mongo_hosts,
    'USER_ID' : 123456,
    'ACCOUNT_ID' : 123456,
    'IS_USING_APIKEY' : False,
}

settings.config.update(config)