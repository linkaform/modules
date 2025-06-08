# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.custom.app import Custom


class Custom(Custom):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.FORM_BITACORA = 134089
        self.CATALOG_CONFIGS = 134765

        self.f.update({
            'fuera_rango_variable': '68376a913f508367405f118f',
            'fuera_rango_valor_obtenido': '68376a913f508367405f1190',
            'fuera_rango_rango_referencia': '68376a913f508367405f1191',
            'group_fuera_de_rango': '68376a3589362d43aac2f3d2',
        })
    