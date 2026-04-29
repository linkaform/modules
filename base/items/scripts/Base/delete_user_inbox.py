# coding: utf-8
import sys, simplejson
from base_utils import Base
from account_settings import *

class Base(Base):
    def delete_user_inbox(self, user_id, record_id):
        cr_couch = self.lkf_api.couch
        # print('cr_couch =',cr_couch)
        cr_db = cr_couch.set_db(f'user_inbox_{user_id}')
        # print('cr_db =',cr_db)
        mango_query = {
            "selector":
            {"record_json._id":record_id},
            "limit":20,"skip":0}
        records = cr_db.find(mango_query)
        for record in records:
            rec = {'_id':record['_id'], '_rev':record['_rev']}
            print('--- rev =',record['_rev'])
            return cr_couch.delete_records(cr_db, [rec,])
        return {"status": 404}

    # def get_user_record(self, folio):
    #     query = {}
    #     return record_id, user_id

    def run_delete_user_inbox(self, data):
        # folio = data.get('folio')
        
        record_id = data.get( 'record_id', data.get('data', {}).get('record_id') )
        user_id = int( data.get( 'user_id', data.get('data', {}).get('user_id') ) )

        # record_id =  '67f80f7e58d318b2db1bd1e7'
        # user_id =  16601

        print('record_id = ',record_id)
        print('user_id = ',user_id)

        # record_id, user_id = self.get_user_record(folio)
        res = self.delete_user_inbox(user_id, record_id)
        return res

if __name__ == "__main__":
    base_obj = Base(settings, sys_argv=sys.argv)
    # base_obj.console_run()
    response = base_obj.run_delete_user_inbox(base_obj.data)

    sys.stdout.write(simplejson.dumps({
        'status': 201,
        'data': response
    }))

