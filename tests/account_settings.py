#   dependencies
from linkaform_api import settings

#   preprod database
settings.mongo_hosts = 'dbs2.lkf.cloud:27918'
settings.mongo_port = 27918
settings.MONGODB_URI = 'mongodb://%s/'%(settings.mongo_hosts)

#   config to industriasmiller database
config = {
    'USERNAME' : 'linkaform@industriasmiller.com',
    'PASS' : '',
    'COLLECTION' : 'form_answer',
    'PROTOCOL' : 'https', #http or https
    'HOST' : 'app.linkaform.com',
    #'PROTOCOL' : 'http', #http or https
    #'HOST' : '192.168.0.25:8000',
    'MONGODB_PORT':settings.mongo_port,
    'MONGODB_HOST': settings.mongo_hosts,
    'MONGODB_USER': 'account_10',
    'PORT' : settings.mongo_port,
    'USER_ID' : 16102,  #   client_id
    'ACCOUNT_ID' : 16102,   #   account_id
    'KEYS_POSITION' : {},
    'IS_USING_APIKEY' : False,
    'USE_JWT' : True,
    'JWT_KEY':'',
    'AUTHORIZATION_EMAIL_VALUE' : 'linkaform@industriasmiller.com',
    'API_KEY':'225bf4f1a58b3ca002b36fa3cda67e13b93f1678',
}

settings.config.update(config)