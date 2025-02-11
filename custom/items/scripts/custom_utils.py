# -*- coding: utf-8 -*-

import sys, simplejson
from datetime import datetime, timedelta, date
from copy import deepcopy

from lkf_addons.addons.custom.app import Custom


class Custom(Custom):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        
        self.ORDEN_DE_TRABAJO = 124767
        self.INVENTARIO_DE_EQUIPOS = 124696
        self.CATALOGO_EQUIPOS_OBJ_ID = "670f23d73ec5f4e5a8fd922e"
        
        self.f = {
            'fecha_orden_trabajo':'67103ee34f6b049508fd9145',
            'frecuencia_mantenimniento':'a00000000000000000000005',
            'fecha_ultimo_mantenimiento':'a00000000000000000000011',
            'fecha_proximo_mantenimiento':'a00000000000000000000012',
            'instrumento_orden_trabajo':'6748b4da70017a0dc61603cc',
            'serie_orden_trabajo':'a00000000000000000000004',
            'grupo_equipos_orden_trabajo':'67103f80eb4737ed96b497bc',
            'codigo_equipo':'a00000000000000000000001',
            }

        