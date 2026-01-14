# coding: utf-8
import sys, simplejson
from linkaform_api import settings
from account_settings import *

import pytz
from datetime import datetime, timedelta


from accesos_utils import Accesos

class Accesos(Accesos):

    def do_access(self, qr_code, location, area, data):
        '''
        Valida pase de entrada y crea registro de entrada al pase
        '''
        access_pass = self.get_detail_access_pass(qr_code)
        if not qr_code and not location and not area:
            return False
        total_entradas = self.get_count_ingresos(qr_code)
        print('self', dir(self))
        user_timezone = self.user.get('timezone', "America/Mexico_City")
        diasDisponibles = access_pass.get("limitado_a_dias", [])
        dias_semana = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
        tz = pytz.timezone(user_timezone)
        hoy = datetime.now(tz)
        dia_semana = hoy.weekday()
        nombre_dia = dias_semana[dia_semana]

        if access_pass.get('estatus',"") == 'vencido':
            self.LKFException({'msg':"El pase esta vencido, edita la información o genera uno nuevo.","title":'Revisa la Configuración'})
        elif access_pass.get('estatus', '') == 'proceso':
            self.LKFException({'msg':"El pase no se ha sido completado aun, informa al usuario que debe completarlo primero.","title":'Requisitos faltantes'})

        if diasDisponibles:
            if nombre_dia not in diasDisponibles:
                dias_capitalizados = [dia.capitalize() for dia in diasDisponibles]

                if len(dias_capitalizados) > 1:
                    dias_formateados = ', '.join(dias_capitalizados[:-1]) + ' y ' + dias_capitalizados[-1]
                else:
                    dias_formateados = dias_capitalizados[0]

                self.LKFException({
                        'msg': f"Este pase no te permite ingresar hoy {nombre_dia.capitalize()}. Solo tiene acceso los siguientes dias: {dias_formateados}",
                        "title":'Aviso'
                    })
        
        limite_acceso = access_pass.get('limite_de_acceso')
        if len(total_entradas) > 0 and limite_acceso and int(limite_acceso) > 0:
            if total_entradas['total_records']>= int(limite_acceso) :
                self.LKFException({'msg':"Se ha completado el limite de entradas disponibles para este pase, edita el pase o crea uno nuevo.","title":'Revisa la Configuración'})
        
        timezone = pytz.timezone(user_timezone)
        fecha_actual = datetime.now(timezone).replace(microsecond=0)
        fecha_caducidad = access_pass.get('fecha_de_caducidad')
        fecha_obj_caducidad = datetime.strptime(fecha_caducidad, "%Y-%m-%d %H:%M:%S")
        fecha_caducidad = timezone.localize(fecha_obj_caducidad)

        # Se agregan 15 minutos como margen de tolerancia
        fecha_caducidad_con_margen = fecha_caducidad + timedelta(minutes=480)

        if fecha_caducidad_con_margen < fecha_actual:
            self.LKFException({'msg':"El pase esta vencido, ya paso su fecha de vigencia.","title":'Advertencia'})
        
        if location not in access_pass.get("ubicacion",[]):
            msg = f"La ubicación {location}, no se encuentra en el pase. Pase valido para las siguientes ubicaciones: {access_pass.get('ubicacion',[])}."
            self.LKFException({'msg':msg,"title":'Revisa la Configuración'})
        
        if self.validate_access_pass_location(qr_code, location):
            self.LKFException("En usuario ya se encuentra dentro de una ubicacion")
        val_certificados = self.validate_certificados(qr_code, location)

        
        pass_dates = self.validate_pass_dates(access_pass)
        comentario_pase =  data.get('comentario_pase',[])
        if comentario_pase:
            values = {self.pase_entrada_fields['grupo_instrucciones_pase']:{
                -1:{
                self.pase_entrada_fields['comentario_pase']:comentario_pase,
                self.mf['tipo_de_comentario']:'caseta'
                }
            }
            }
            # self.update_pase_entrada(values, record_id=[str(access_pass['_id']),])
        res = self._do_access(access_pass, location, area, data)
        return res
    
if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    #-FILTROS
    data = acceso_obj.data.get('data',{})
    option = data.get("option",'')

    area = data.get("area")
    comments = data.get('comments',"")
    checkin_id = data.get("checkin_id", "")
    employee_list = data.get("employee_list",[])
    equipo = data.get('equipo',"")
    forzar = data.get('forzar')
    marca = data.get('marca',"")
    guards = data.get('guards',[])
    location = data.get("location")
    qr_code = data.get('qr_code')
    record_id = data.get('record_id')
    support_guards = data.get('support_guards')
    tipo = data.get('tipo',"")
    vehiculo = data.get('vehiculo',"")
    visita_a = data.get('visita_a',"")
    gafete_id = data.get('gafete_id',"")
    data_msj=data.get("data_msj", {})
    data_cel_msj=data.get("data_cel_msj", {})
    status_visita=data.get("status_visita", "")
    inActive= data.get("inActive", "")
    turn_areas= data.get("turn_areas", True)
    prioridades = data.get("prioridades",[])
    id_bitacora = data.get("id_bitacora",[])
    data_gafete = data.get("data_gafete",{})
    tipo_movimiento = data.get("tipo_movimiento",{})
    dateFrom = data.get("dateFrom", "")
    dateTo = data.get("dateTo", "")
    filterDate = data.get("filterDate", "")
    limit = data.get("limit", 10)
    offset = data.get("offset", 0)
    fotografia=data.get("fotografia",[])
    nombre_suplente=data.get("nombre_suplente","")
    guard_id=data.get("guard_id","")
    #-FUNCTIONS
    print('option', option)
    if option == 'load_shift':
        # used
        response = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
    elif option == 'assets_access_pass':
        response = acceso_obj.assets_access_pass(location)
    elif option == 'assing_gafete':
        response = acceso_obj.assing_gafete(data_gafete, id_bitacora, tipo_movimiento)
    elif option == 'list_bitacora':
        response = acceso_obj.get_list_bitacora(location,  area, prioridades=prioridades, dateFrom=dateFrom, dateTo=dateTo, limit=limit, offset=offset,  filterDate=filterDate)
    elif option == 'list_bitacora2':
        response = acceso_obj.get_list_bitacora2(location,  area, prioridades=prioridades, dateFrom=dateFrom, dateTo=dateTo, filterDate=filterDate)
    elif option == 'get_user_booths':
        response = acceso_obj.get_user_booths_availability(turn_areas=turn_areas)
    elif option == 'get_boot_guards' or option == 'guardias_de_apoyo':
        response = acceso_obj.get_booths_guards(location, area, solo_disponibles=True, **{'position':acceso_obj.support_guard})
    elif option == 'catalog_estado':
        response = acceso_obj.catalogo_estados()
    elif option == 'catalog_location':
        response = acceso_obj.get_catalog_locations(location)
    elif option == 'checkin':
        response = acceso_obj.do_checkin(location, area, employee_list, fotografia=fotografia ,nombre_suplente=nombre_suplente, checkin_id=checkin_id)
    elif option == 'checkout':
        # used
        response = acceso_obj.do_checkout(checkin_id=checkin_id, \
            location=location, area= area, guards=guards, forzar=forzar, comments=comments, fotografia=fotografia, guard_id=guard_id)
    elif option == 'get_user_menu':
        response = acceso_obj.get_config_accesos()
    elif option == 'search_access_pass':
        # used
        response = acceso_obj.search_access_pass(qr_code=qr_code, location=location)
    elif option == 'lista_pases':
        # used
        response = acceso_obj.get_lista_pase(location=location, inActive=inActive)
    elif option == 'do_out':
        # used
        response = acceso_obj.do_out(qr_code, location, area, gafete_id)
    elif option == 'do_access':
        # used
        response = acceso_obj.do_access(qr_code, location, area, data)
    elif option == 'update_bitacora_entrada':
        # used
        response = acceso_obj.update_bitacora_entrada(data, record_id=record_id)
    elif option == 'update_bitacora_entrada_many':
        # used
        response = acceso_obj.update_bitacora_entrada_many(data, record_id=record_id)
    elif option == 'notes_guard':
        response = acceso_obj.get_guard_notes(location, booth)
    elif option == 'vehiculo_tipo':
        if tipo and marca:
            response = acceso_obj.vehiculo_modelo(tipo, marca)
        elif tipo:
            response = acceso_obj.vehiculo_marca(tipo)
        else:
            response = acceso_obj.vehiculo_tipo()
    elif option == 'create_pase' or option == 'crear_pase':
        response = acceso_obj.create_access_pass(data_pase)
    elif option == 'update_guards':
        response = acceso_obj.update_guards_checkin(support_guards, checkin_id, location, area)
    elif option == 'visita_a':
        response = acceso_obj.visita_a(location)
    elif option == 'visita_a_detail':
        response = acceso_obj.visita_a_detail(location, visita_a)
    elif option == 'enviar_msj':
        response = acceso_obj.create_enviar_msj(data_msj=data_msj, data_cel_msj=data_cel_msj)
    elif option == 'send_msj_by_access':
        response = acceso_obj.send_email_and_sms(data=data_msj)
    elif option == 'update_delete_suplente':
        response = acceso_obj.update_delete_suplente(nombre_suplente=nombre_suplente)
    else :
        response = {"msg": "Empty"}
    # print('================ END RETURN =================')
    # print(simplejson.dumps(response, indent=3))
    acceso_obj.HttpResponse({"data":response})

