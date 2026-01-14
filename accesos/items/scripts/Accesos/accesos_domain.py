# -*- coding: utf-8 -*-
"""
Script utilizado en Testing con Pytest para manejar la logica de las distintas secciones del modulo de Accesos.
    Turnos
    Accesos
    Incidencias
    Fallas
    Rondines
    Articulos Concesionados
    Pases
"""
import sys, simplejson
from linkaform_api import settings
from account_settings import *
from accesos_utils import Accesos

# uncomment below line for testing purposes - dont remove!!!
# from lkf_modules.accesos.items.scripts.Accesos.accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

def get_shift_data(acceso_obj, data):
    return acceso_obj.get_shift_data(
        booth_location=data.get("location"),
        booth_area=data.get("area")
    )

def do_checkin(acceso_obj, data):
    return acceso_obj.do_checkin(
        location=data.get("location"),
        area=data.get("area"), 
        employee_list=data.get("employee_list"),
        fotografia=data.get("fotografia"),
        nombre_suplente=data.get("nombre_suplente"),
        checkin_id=data.get("checkin_id")
    )

def do_checkout(acceso_obj, data):
    return acceso_obj.do_checkout(
        checkin_id=data.get("checkin_id"),
        location=data.get("location"), 
        area=data.get("area"),
        guards=data.get("guards"),
        forzar=data.get("forzar", False),
        comments=data.get("comments", ""),
        fotografia=data.get("fotografia")
    )

DISPATCHER = {
    "load_shift": get_shift_data,
    "do_checkin": do_checkin,
    "do_checkout": do_checkout,
}

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()

    data = acceso_obj.data.get("data", {})
    option = data.get("option")

    handler = DISPATCHER.get(option)

    if not handler:
        response = {"error": f"Option '{option}' not supported"}
    else:
        response = handler(acceso_obj, data)

    acceso_obj.HttpResponse({"data": response})
