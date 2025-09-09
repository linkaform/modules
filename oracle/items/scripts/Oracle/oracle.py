# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base

from linkaform_api import base
from lkf_addons.addons.employee.app import Employee
from lkf_addons.addons.activo_fijo.app import Vehiculo

from lkf_addons.addons.oracle.app import Oracle

class Oracle(Oracle):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.name =  __class__.__name__
        self.settings = settings
        self.f.update({
            'email_cliente_2':'68c050237fcf44c0212517e0',
            'email_cliente_3':'68c050237fcf44c0212517e1',
        })


    def sync_db_catalog(self, db_name, query={}):
        header, data = self.query_view(db_name, query=query)
        return header, data