# -*- coding: utf-8 -*-
import sys, simplejson
from linkaform_api import settings, utils
from magnolia_settings import *


FORM_CATALOG_DIR = {
    81503:80755, #Inventory Flow (lab)
    98225:98230, #Inventory Flow (greenHouse)
    }

def get_record_catalog(form_id, folio=None):

    mango_query = {
        "selector":{"answers": {}},
        "limit":1000,
        "skip":0
        }
    if folio:
        mango_query['selector']['answers'].update({'62c44f96dae331e750428732':folio})
    else:
        mango_query['selector']['answers'].update({'620ad6247a217dbcb888d175': "Done"})
        
    res = lkf_api.search_catalog( FORM_CATALOG_DIR[form_id], mango_query, jwt_settings_key='USER_JWT_KEY')
    return res

def del_catalog_record(record_catalog):
    if record_catalog:
        for info_record_catalog in record_catalog:
            print('info_record_catalog', info_record_catalog)
            resp_delete = lkf_api.delete_catalog_record(FORM_CATALOG_DIR[form_id], info_record_catalog.get('_id'), info_record_catalog.get('_rev'), jwt_settings_key='USER_JWT_KEY')
            print('resp_delete=',resp_delete)
            return resp_delete



if __name__ == '__main__':
    print(sys.argv)
    current_record = simplejson.loads( sys.argv[1] )
    form_id = current_record.get('form_id')
    jwt_complete = simplejson.loads( sys.argv[2] )
    config['USER_JWT_KEY'] = jwt_complete["jwt"].split(' ')[1]
    settings.config.update(config)
    lkf_api = utils.Cache(settings)

    #current record
    record_catalog = get_record_catalog( form_id, folio=current_record['folio'] )
    del_catalog_record(record_catalog)
    #doing cleanup
    #for done records that for some reason are not deleted
    record_catalog = get_record_catalog(form_id)
    del_catalog_record(record_catalog)

