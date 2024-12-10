# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta
from linkaform_api import settings, network, utils

from lkf_addons.addons.base.app import Schedule

from account_settings import *


class Schedule(Schedule):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):

        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)

        self.mf.update({
            'pais_localidad':'631fccdd844ed53c7d989718',
            'localidad':'631fc1e48d9fe191da0c3daf',
            })


        self.LOCALIDADES = self.lkm.catalog_id('localidades')
        self.LOCALIDADES_ID = self.LOCALIDADES.get('id')
        self.LOCALIDADES_OBJ_ID = self.LOCALIDADES.get('obj_id')