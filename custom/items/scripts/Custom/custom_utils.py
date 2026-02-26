# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, timezone
from copy import deepcopy

from lkf_addons.addons.custom.app import Custom


class Custom(Custom):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.format_date = '%Y-%m-%d %H:%M:%S'
        self.user_id = self.user['user_id']
        self.form_id_checkin = 145667

    def get_end_dates(self):
        # datos del registro
        end_ts = self.current_record['end_timestamp']
        offset_min = self.current_record.get('tz_offset', 0)

        # datetime local del registro
        dt_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc)
        dt_local = dt_utc + timedelta(minutes=offset_min)

        # obtener las 00:00 locales de esa fecha
        dt_local_midnight = dt_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # convertir esas 00:00 locales a UTC
        dt_midnight_utc = dt_local_midnight - timedelta(minutes=offset_min)

        return dt_local.strftime(self.format_date), dt_midnight_utc.strftime(self.format_date)

    def get_record_check_in_today(self, str_date_from, get_catalog_data=False):
        data_to_get = {'folio': 1, 'created_at': 1}
        if get_catalog_data:
            data_to_get['answers.69667a148942065f5657f075'] = 1
        return self.cr.find_one({
            'form_id': self.form_id_checkin,
            'deleted_at': {'$exists': False},
            'created_at': {'$gte': datetime.strptime( str_date_from, self.format_date )},
            'answers.a00000000000000000000007': 'check_in',
            'created_by_id': self.user_id
        }, data_to_get)