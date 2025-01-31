# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta

#from account_utils import get_inventory_flow
from linkaform_api import network, utils
from lkf_addons.addons.stock_greenhouse.app import Stock

from account_settings import *



class Stock(Stock):


    def grading_calcs(self):
        total_graded = 0

        for gset_grading in self.answers.get(self.f['grading_group'],[]):
            ready_year = gset_grading.get(self.f['grading_ready_year'])
            ready_week = gset_grading.get(self.f['grading_ready_week'])
            lot_number = int(f"{ready_year}{ready_week:02}")
            gset_grading[self.f['grading_ready_yearweek']] = lot_number

        for gset in self.answers.get(self.f['production_group'],[]):
            start_time = gset.get(self.f['set_production_date'])
            end_time = gset.get(self.f['time_out'])
            total_hours = self.calc_work_hours(gset)
            gset[self.f['set_total_hours']] = round(total_hours, 2)
            containers_out = gset[self.f['set_total_produced']]
            if not containers_out:
                msg = "Es necesario indicar cuantos flats hizo"
                msg_error_app = {
                    f"{self.f['set_total_produced']}": {
                        "msg": [msg],
                        "label": "Please check your flats graded",
                        "error":[]
      
                    }
                    }
                self.LKFException(msg)
            total_eu = 1 * containers_out 
            flats_per_hour = total_eu / float(total_hours)
            total_graded += total_eu
            gset[self.f['set_products_per_hours']] = round(flats_per_hour, 2) # Plants per Hour

        self.answers[self.f['total_produced']] = total_graded
        self.answers[self.f['inv_scrap_status']] = 'done'


if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    current_record = stock_obj.current_record
    if not stock_obj.record_id:
        stock_obj.record_id = stock_obj.object_id()   
    answers = stock_obj.grading_calcs()
    stock_obj.do_scrap()

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'replace_ans': stock_obj.answers,
    }))


