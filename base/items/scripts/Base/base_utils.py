# -*- coding: utf-8 -*-
import sys, simplejson
from datetime import datetime, timedelta
from linkaform_api import settings, network, utils

from lkf_addons.addons.base.app import Schedule

from account_settings import *


class Base(base.LKF_Base):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        print('entra a este base')
        # self.load(module='Base', module_class='Schedule', import_as='Schedule', **self.kwargs)
        self.sending_user = {}
        self.inboxes_groups = []
        self.mf.update(
            {
            'inboxes':'67567ea740a55b8a1c487f23'
            })

        self.mf.update({
            'pais_localidad':'631fccdd844ed53c7d989718',
            'localidad':'631fc1e48d9fe191da0c3daf',
            })


        self.LOCALIDADES = self.lkm.catalog_id('localidades')
        self.LOCALIDADES_ID = self.LOCALIDADES.get('id')
        self.LOCALIDADES_OBJ_ID = self.LOCALIDADES.get('obj_id')


    def send_email_by_form_answers(self, data):
        #Inherit Function / Heredar Funcion
        '''
        Se hereda esta funcion para modifcar el answer que envia el correo.
        '''
        answers = super().send_email_by_form_answers(data)
        if hasattr(self, 'inboxes_groups'):
            answers[f"{self.mf['inboxes']}"] = self.inboxes_groups
        return answers
