# coding: utf-8
#!/usr/bin/python

#####
# Made by Roman Lezama
#####
from linkaform_api import settings
#settings.mongo_hosts = 'db2.linkaform.com:27017,db3.linkaform.com:27017,db4.linkaform.com:27017'
#settings.mongo_port = 27017
settings.mongo_hosts = 'dbs2.lkf.cloud:27918'
settings.mongo_port = 27918
#settings.mongo_hosts = '192.168.0.25:27017'
#settings.mongo_port = 27017
settings.MONGODB_URI = 'mongodb://%s/'%(settings.mongo_hosts)

config = {
    'USERNAME' : 'josepato@linkaform.com',
    'PASS' : '',
    'COLLECTION' : 'form_answer',
    'PROTOCOL' : 'https', #http or https
    'HOST' : 'preprod.linkaform.com',
    #'PROTOCOL' : 'http', #http or https
    #'HOST' : '192.168.0.25:8000',
    'MONGODB_PORT':settings.mongo_port,
    'MONGODB_HOST': settings.mongo_hosts,
    'MONGODB_USER': 'account_126',
    'PORT' : settings.mongo_port,
    'USER_ID' : 126,
    'ACCOUNT_ID' : 126,
    'KEYS_POSITION' : {},
    'IS_USING_APIKEY' : False,
    'USE_JWT' : True,
    'JWT_KEY':'',
    'AUTHORIZATION_EMAIL_VALUE' : 'josepato@linkaform.com',
    'API_KEY':'2168e86f66c35859c353fc056a3793fb1449fd97',
}

settings.config.update(config)
