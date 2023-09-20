# -*- coding: utf-8 -*-
import sys, simplejson

from linkaform_api import settings, network, utils
from account_settings import *


INVENTORY_FORM_ID = 98225



if __name__ == '__main__':
    print(sys.argv)
    current_record = simplejson.loads( sys.argv[1] )
    jwt_complete = simplejson.loads( sys.argv[2] )
    config['JWT_KEY'] = jwt_complete['jwt'].split(' ')[1]
    settings.config.update(config)
    lkf_api = utils.Cache(settings)
    current_answers = current_record['answers']
    plant_info = current_answers.get('6442cbafb1b1234eb68ec178',{})
    folio_inventory = plant_info.get('62c44f96dae331e750428732')
    a = lkf_api.patch_multi_record({'620ad6247a217dbcb888d175':'active'}, INVENTORY_FORM_ID, folios=[folio_inventory])
