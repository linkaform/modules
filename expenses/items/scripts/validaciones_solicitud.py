# -*- coding: utf-8 -*-
import sys, wget, bson, simplejson
from datetime import datetime, date

#from expense_utils import Expenses
from lkf_addons.addons.expenses.expense_utils import Expenses

from account_settings import *



if __name__ == '__main__':
    print(sys.argv)
    current_record = simplejson.loads(sys.argv[1])
    jwt_complete = simplejson.loads( sys.argv[2] )
    config['JWT_KEY'] = jwt_complete['jwt'].split(' ')[1]
    settings.config.update(config)
    expense_obj = Expenses(settings)

    current_record['answers'] = expense_obj.validaciones_solicitud(current_record['answers'])
   
    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': current_record['answers']
    }))