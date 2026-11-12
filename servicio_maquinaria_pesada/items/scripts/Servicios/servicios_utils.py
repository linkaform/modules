# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.servicio_maquinaria_pesada.app import ServicioMaquinariaPesada


class ServicioMaquinariaPesada(ServicioMaquinariaPesada):

    def __init__(self, settings, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
