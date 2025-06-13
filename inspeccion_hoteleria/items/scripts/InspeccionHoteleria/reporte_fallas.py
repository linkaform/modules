# -*- coding: utf-8 -*-
from ast import mod
import enum
from pickle import NONE
import re
import sys, simplejson
import datetime
import unicodedata

from inspeccion_hoteleria_utils import Inspeccion_Hoteleria

from account_settings import *
from bson import ObjectId
import datetime

class Inspeccion_Hoteleria(Inspeccion_Hoteleria):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)

        self.cr_inspeccion = self.net.get_collections(collection='inspeccion_hoteleria')

        self.f.update({
            'tipo_ubicacion': '683d2970bb02c9bb7a0bfe1c',
            'habitacion_remodelada': '67f0844734855c523e1390d7',
            'ubicacion_nombre': '663e5c57f5b8a7ce8211ed0b',
            'tipo_area': '663e5e68f5b8a7ce8211ed18',
            'status_auditoria': '67f0844734855c523e139132',
            'tipo_area_habitacion': '663e5e68f5b8a7ce8211ed18',
            'numero_habitacion': '680977786d022b9bff6e3645',
            'piso_habitacion': '680977786d022b9bff6e3646',
            'nombre_area_habitacion': '663e5d44f5b8a7ce8211ed0f',
            'nombre_camarista': '67f0844734855c523e1390d6'
        })

        self.form_ids = {
            'revison_habitacion': self.REVISON_HABITACION,
            'hi_parque_fundidora': self.HI_PARQUE_FUNDIDORA,
            'crowne_plaza_torreon': self.CROWNE_PLAZA_TORREN,
            'travo': self.TRAVO,
            'hie_tecnologico': self.HIE_TECNOLGICO,
            'wyndham_garden_mcallen': self.WYNDHAM_GARDEN_MCALLEN,
            'istay_victoria': self.ISTAY_VICTORIA,
            'hie_torreon': self.HIE_TORREN,
            'hilton_garden_silao': self.HILTON_GARDEN_SILAO,
            'ms_milenium': self.MS_MILENIUM,
            'istay_monterrey_historico': self.ISTAY_MONTERREY_HISTRICO,
            'hie_guanajuato': self.HIE_GUANAJUATO,
            'hie_silao': self.HIE_SILAO,
            'istay_ciudad_juarez': self.ISTAY_CIUDAD_JUREZ,
            'crowne_plaza_mty': self.CROWNE_PLAZA_MONTERREY,
            'hie_galerias': self.HIE_GALERIAS,
            'holiday_inn_tijuana': self.HOLIDAY_INN_TIJUANA,
        }

        self.hotel_name_abreviatura = {
            'HILTON GARDEN SILAO': 'HGI SILAO',
            'HOLIDAY INN TIJUANA': 'HI TIJUANA ZONA RIO'
        }

        self.fallas_hotel = {
            'foco_de_pasillo_afuera_de_la_habitacion': '67f0844734855c523e1390d8',
            'plafon_fuera_de_la_habitacion': '67f0844734855c523e1390db',
            'esquinero': '67f0844734855c523e1390de',
            'placa_de_numero_de_habitacion': '67f0844734855c523e1390e1',
            'placa_de_no_fumar': '67f0844734855c523e1390e4',
            'timbre': '67f7e39ceffd2a15e842ba54',
            'puerta_de_entrada_exterior': '67f0844734855c523e1390ea',
            'chapa_electronica_exterior': '67f0844734855c523e1390ed',
            'contra_chapa': '67f0844734855c523e1390f0',
            'puerta_de_entrada_interior': '67f0844734855c523e1390f3',
            'bisagras_de_puerta_de_entrada': '67f0844734855c523e1390f6',
            'marco_de_puerta_de_entrada': '67f0844734855c523e1390f9',
            'sardinel': '67f0844734855c523e1390fc',
            'auto_cierre_de_puerta_de_entrada': '67f0844734855c523e1390ff',
            'buen_olor': '67f0844734855c523e139102',
            'mirilla': '67f0844734855c523e139105',
            'plano_evacuacion': '67f0844734855c523e139108',
            'hueso_de_pollo_/_gomita': '67f0844734855c523e13910b',
            'chapa_interior_-_pasador': '67f0844734855c523e13910e',
            'tope_de_puerta_de_entrada': '67f0844734855c523e139111',
            'colgante_no_molestar': '67f0844734855c523e139114',
            'colgante_greener_stay': '67f0844734855c523e139117',
            'guardapolvo': '67f0844734855c523e13911a',
            'apagador_de_entrada_/_switch_maestro': '67f0844734855c523e13911d',
            'ahorrador_de_energia': '680bb9ae683baa875e7baa1f',
            'luminaria_de_entrada': '67f0844734855c523e139120',
            'plafon_entrada': '67f0844734855c523e139123',
            'piso_laminado / Alfombra entrada': '67f0844734855c523e139126',
            'zoclo_de_entrada': '67f0844734855c523e139129',
            'tapiz_/_pintura_de_entrada': '67f0844734855c523e13912c',
            'doble_puerta_(Chapa,_marco,_cinta tesamol)': '67f7e39ceffd2a15e842ba57',
            'puerta_de_comunicacion': '67ed63bafba6620598f318a2',
            'chapa_/_seguro': '67ed63f3f7b2bd2aa34d9e6a',
            'contra_chapa': '67ed63f3f7b2bd2aa34d9e6b',
            'tapa_ciega': '67ed63f3f7b2bd2aa34d9e6c',
            'marco_de_puerta_de_comunicacion': '67ed63f3f7b2bd2aa34d9e6d',
            'cinta_tesamol': '67ed63f3f7b2bd2aa34d9e6e',
            'luminaria_de_closet': '67ed64ce17c7a71475f318d2',
            'puertas_de_closet': '674a006c98371866c7160370',
            'manijas_/perillas_de_puerta_de_closet': '67ed64ce17c7a71475f318d3',
            'marco_de_puertas_de_closet': '67ed64ce17c7a71475f318d4',
            'broches_perico': '67ed64ce17c7a71475f318d5',
            'plafon_de_closet': '67ed64ce17c7a71475f318d6',
            'rack_tubular': '67ed64ce17c7a71475f318d7',
            'ganchos_(lisos_y_con_pinza)': '674a001bcc56e7f30c939286',
            'bolsa_de_lavanderia': '674a006c98371866c716036a',
            'lista_de_lavanderia': '674a006c98371866c716036b',
            'rack_para_plancha': '674a006c98371866c716036c',
            'plancha': '674a006c98371866c716036f',
            'tabla_de_planchar': '674a006c98371866c716036e',
            'piso_laminado_/_alfombra_de_closet': '674a006c98371866c716036d',
            'zoclo_de_closet': '674a006c98371866c7160371',
            'tapiz_/_pintura_de_closet': '67ed64ce17c7a71475f318d8',
            'caja_de_seguridad': '67ed64ce17c7a71475f318d9',
            'instrucciones_de_caja_de_seguridad': '67ed64ce17c7a71475f318da',
            'porta_equipaje_(tijera_o_integrado_en_mueble_de_TV)': '67ed64ce17c7a71475f318db',
            'detector_de_humo': '67ed650d8ec7fb6f794d9ee7',
            'bocina': '67ed650d8ec7fb6f794d9ee8',
            'sprinkle': '67ed650d8ec7fb6f794d9ee9',
            'luz_estroboscopica_(solo_en_habitacion_de_discapacitados)': '67ed650d8ec7fb6f794d9eea',
            'habitacion_para_discapacitados_(mirilla,_silla_portatil_en_baño,_lavabo_adaptado,_telefono_especial,_push_en_cajones,_push_de_lampara,_aro_en_bastones_de_cortina)': '67ed650d8ec7fb6f794d9eeb',
            'puerta_de_baño': '6749fe84b841cb116d0755eb',
            'apagador_de_baño': '67f035888532823bf4138dc0',
            'bisagras_de_puerta_de_baño': '6749ffcb33e84f4eb44a19bd',
            'colgante_de_programa_de_reuso_de_toallas_(en_chapa,_tubo_curvo_o_gancho)': '6749ffcb33e84f4eb44a19be',
            'tope_de_puerta_de_baño': '6749ffcb33e84f4eb44a19bf',
            'marco_de_puerta_de_baño': '6749ffcb33e84f4eb44a19c0',
            'sardinel': '6749ffcb33e84f4eb44a19c1',
            'chapa_exterior_/_interior': '6749ffcb33e84f4eb44a19c2',
            'contra_chapa_de_puerta_de_baño': '6749ffcb33e84f4eb44a19c3',
            'espejo_de_cuerpo_entero': '6749ffcb33e84f4eb44a19c4',
            'gancho': '6749ffcb33e84f4eb44a19c5',
            'plafon_de_baño': '6749ffcb33e84f4eb44a19c6',
            'muros_de_baño': '6749ffcb33e84f4eb44a19c7',
            'wc(_sanitario,_tapa,_herrajes)': '6749ffcb33e84f4eb44a19c8',
            'valvulas_de_paso_(sin_fuga)': '6749ffcb33e84f4eb44a19c9',
            'cesto_de_basura_de _baño': '6749ffcb33e84f4eb44a19ca',
            'papel_higienico': '6749ffcb33e84f4eb44a19cb',
            'portarrollos': '6749ffcb33e84f4eb44a19cc',
            'piso_de_baño': '6749ffcb33e84f4eb44a19cd',
            'coladera_de_piso_de_baño': '6749ffcb33e84f4eb44a19ce',
            'zoclo_de_baño': '6749ffcb33e84f4eb44a19cf',
            'rejilla_de_extraccion': '6749ffcb33e84f4eb44a19d0',
            'rack_toallero_o_marimba': '6749ffcb33e84f4eb44a19d1',
            'toallas_de_Baño': '67ed6592b4c33b95759a4f77',
            'luminaria_de_tina': '67ed662ab178b35ea5a103b0',
            'tubo_curvo': '67ed662ab178b35ea5a103b1',
            'cortina_de_baño': '67ed662ab178b35ea5a103b2',
            'tapete_de_felpa': '67ed662ab178b35ea5a103b3',
            'sin_tendedero_retractil': '67ed662ab178b35ea5a103b4',
            'paredes_de_tina': '67ed662ab178b35ea5a103b5',
            'cebolleta': '67ed662ab178b35ea5a103b6',
            'monomando_/_llaves_de_regadera': '67ed662ab178b35ea5a103b7',
            'agua_fria_/_caliente': '67ed662ab178b35ea5a103b8',
            'desague': '67ed662ab178b35ea5a103b9',
            'barras_de_seguridad': '67ed662ab178b35ea5a103ba',
            'jabonera': '680bbf54e262646da62818f7',
            'jabon_de_cuerpo': '680bbf54e262646da62818fa',
            'rack_dispensador': '67ed662ab178b35ea5a103bd',
            'botellas_de_shampoo_-_acondicionador_-_gel_de_baño': '67ed662ab178b35ea5a103be',
            'nariz_de_tina': '67ed662ab178b35ea5a103bf',
            'automatico_de_tina_/_push': '67ed662ab178b35ea5a103c0',
            'tina_base_(antiderrapante)_y_faldon': '67ed662ab178b35ea5a103c1',
            'tapon_de_tina': '67ed662ab178b35ea5a103c2',
            'cristal_de_regadera,_soporte_de_regadera,_puerta': '680bbf54e262646da62818fd',
            'chapeton': '67ed662ab178b35ea5a103c3',
            'emboquillado_(silicon)': '67ed662ab178b35ea5a103c4',
            'luminarias_de_lavabo': '67ed67dfc61cd5bd89f317f2',
            'espejo_de_Lavabo': '67ed67dfc61cd5bd89f317f3',
            'sin_brackets': '680bc1bce1e8a2ba776c38fa',
            'espejo_de_vanidad': '67ed67dfc61cd5bd89f317f4',
            'lavabo_superficie_y_faldon': '67ed67dfc61cd5bd89f317f5',
            'ovalin': '67ed67dfc61cd5bd89f317f6',
            'monomando_/_llave_mezcladora': '67ed67dfc61cd5bd89f317f7',
            'automatico_de_lavabo': '67ed67dfc61cd5bd89f317f8',
            'agua_fria_/_Caliente': '67ed67dfc61cd5bd89f317f9',
            'desague': '67ed67dfc61cd5bd89f317fa',
            'olvido_algo_-_tent_card': '680bc1bce1e8a2ba776c38fd',
            'kleenera': '67ed67dfc61cd5bd89f317fc',
            'kleenex_-panuelos': '67ed67dfc61cd5bd89f317fd',
            'contacto_falla_tierra': '67f0398ed7a42953a0cbaef3',
            'secadora_de_pelo': '67ed67dfc61cd5bd89f317fe',
            'bolsa_de_secadora': '67f0398ed7a42953a0cbaef4',
            'amenidades_(solo_vIPS)': '680bc1bce1e8a2ba776c3900',
            'rack-_dispensador': '67ed67dfc61cd5bd89f31800',
            'botellas_de_jabon_de_manos_y_crema corporal': '67ed67dfc61cd5bd89f31801',
            'jabon_facial_y_crema_corporal': '680bc1bce1e8a2ba776c3903',
            'toallas_(_manos,_facial)': '67ed67dfc61cd5bd89f31803',
            'aro_o_rack_toallero': '67ed67dfc61cd5bd89f31804',
            'valvulas_(sin_fuga)': '67ed67dfc61cd5bd89f31805',
            'trampa_de_lavabo_/_tubo_cespol': '67ed67dfc61cd5bd89f31806',
            'rejilla_de_inyeccion': '674a00cf5f05ce94969392ba',
            'rejilla_de_retorno': '674a0257ce0d9173204a19c5',
            'apagadores': '674a0257ce0d9173204a19c6',
            'piso_/_alfombra': '674a0257ce0d9173204a19c7',
            'tapiz_/_pintura_de_muros': '674a0257ce0d9173204a19c8',
            'zoclos': '674a0257ce0d9173204a19c9',
            'a_/_c_(sin_ruido)': '674a0257ce0d9173204a19ca',
            'termostato': '674a0257ce0d9173204a19cb',
            'lampara_de_pie': '674a0257ce0d9173204a19cc',
            'cabecera': '674a0257ce0d9173204a19cd',
            'almohadas_(funda_y_protector)': '674a0257ce0d9173204a19ce',
            'ropa_de_cama_(sabanas,_duvet,_rodapie,_etc)': '674a0257ce0d9173204a19cf',
            'tendido_de_cama': '674a0257ce0d9173204a19d0',
            'perchero': '680bca79343dc15af751bbc9',
            'buros_(cajones)': '674a0257ce0d9173204a19d1',
            'multi-_contacto': '674a0257ce0d9173204a19d2',
            'lamparas_buro_o_lampara_de_cabecera': '674a0257ce0d9173204a19d3',
            'luz_de_lectura': '67ffebc902d1b72e5551bbf9',
            'reloj_despertador_/_Cubitime': '674a0257ce0d9173204a19d4',
            'telefono_/_caratula': '674a0257ce0d9173204a19d5',
            'tent_card_reuso_de_sabanas': '674a0257ce0d9173204a19d6',
            'cuadros_decorativos': '674a0257ce0d9173204a19d7',
            'sofa_doble_/_sillon_individual_(hab_sencilla)': '680bca79343dc15af751bbcc',
            'sillas_(hab_sencilla)': '67f03e568d1b646b9e365acc',
            'sillon_(hab_doble)': '674a0257ce0d9173204a19d9',
            'mesa_de_apoyo': '674a0257ce0d9173204a19da',
            'bandeja_para_cafetera': '674a0257ce0d9173204a19db',
            'cafetera': '674a0257ce0d9173204a19dc',
            'cafe _(regular_y_descafeinado)': '674a0257ce0d9173204a19dd',
            'te': '674a0257ce0d9173204a19de',
            'kit_de_complementos': '674a0257ce0d9173204a19df',
            'tazas_de_ceramica_con_blonda': '67ed67dfc61cd5bd89f31809',
            'vasos_vidrio_con_blonda': '67ed67dfc61cd5bd89f3180a',
            'botellas_con_agua': '67ed67dfc61cd5bd89f3180b',
            'vaso_termico_para_cafe_en_bolsa': '680bca79343dc15af751bbcf',
            'vaso_desechable_con_tapa_en_bolsa': '680bca79343dc15af751bbd2',
            'hielera_con_bolsa_y_tapa': '67ed67dfc61cd5bd89f3180e',
            'cortinas_de_frescura': '67ed67dfc61cd5bd89f31811',
            'cortinas_black out': '67ed67dfc61cd5bd89f31812',
            'cortineros_-_riel': '67ed67dfc61cd5bd89f31813',
            'bastones_de_cortinas': '67ed67dfc61cd5bd89f31814',
            'ventanal_cerrado_/_pasamanos': '67f08a48e97471d4cef84404',
            'balcon': '680bca79343dc15af751bbd5',
            'perno_de_seguridad_balcon': '680bca79343dc15af751bbd8',
            'rieles_de_ventanal_de_balcon': '680bca79343dc15af751bbdb',
            'barandal_de_balcon': '680bca79343dc15af751bbde',
            'sardinel_de_balcon': '680bca79343dc15af751bbe1',
            'sillas_de_balcon': '680bca79343dc15af751bbe4',
            'macetas_decorativas': '680bca79343dc15af751bbe7',
            'internet': '67ffed2115a48b68c37ba998',
            'sin_insectos': '67ffed2115a48b68c37ba99b',
            'lampara_de_escritorio': '674a0312d080d8ec99075621',
            'multi-_contactos': '674a033bce0d9173204a1a16',
            'escritorio': '674a033bce0d9173204a1a17',
            'silla_de_escritorio_/_ergonomica': '674a033bce0d9173204a1a18',
            'reglamento_interno': '67ed67dfc61cd5bd89f3181a',
            'tips_de_seguridad': '67ed67dfc61cd5bd89f3181b',
            'cesto_de_basura': '67ed67dfc61cd5bd89f3181c',
            'mueble': '674a03706d15fa34f3828411',
            'television_(imagen,_volumen)': '674a03c186f66205504a1a55',
            'guia_de_canales': '674a03c186f66205504a1a56',
            'control_remoto': '674a03c186f66205504a1a57',
            'directorio_de_servicios_qr': '674a03c186f66205504a1a5a',
            'cajones': '674a03c186f66205504a1a58',
            'menu_de_room_service': '674a03c186f66205504a1a59',
            'rejilla_de_inyeccion': '67f04106303b619b9cf842c5',
            'rejilla_de_retorno': '67f04218dd311d4ea0138e2c',
            'apagadores': '67f04218dd311d4ea0138e2d',
            'piso_/_alfombra': '67f04218dd311d4ea0138e2e',
            'tapiz_/_pintura de muros': '67f04218dd311d4ea0138e2f',
            'cuadros_decorativos': '67ffef2aca29d5ca7b7ba9c7',
            'zoclos': '67f04218dd311d4ea0138e30',
            'a_/_C_(sin_ruido)': '67f04218dd311d4ea0138e31',
            'termostato_de_sala': '67f04218dd311d4ea0138e32',
            'lampara_de_pie_de_sala': '67f04218dd311d4ea0138e33',
            'mueble_-_sofa_de_sala': '67f04218dd311d4ea0138e34',
            'tV sala suite': '680bca79343dc15af751bbec',
            'mesa_de_centro': '67f04218dd311d4ea0138e35',
            'mesa_de_comedor': '67ffef2aca29d5ca7b7ba9c8',
            'sillas_de_comedor': '67ffef2aca29d5ca7b7ba9cb',
            'lampara_de_techo_/_socket': '67ffef2aca29d5ca7b7ba9ce',
            'microondas': '67f04218dd311d4ea0138e36',
            'frigobar': '67f04218dd311d4ea0138e37',
            'cocineta': '67f04218dd311d4ea0138e38',
            'tarja': '680bca79343dc15af751bbef',
        }

    def normalize_types(self, obj):
        if isinstance(obj, list):
            return [self.normalize_types(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self.normalize_types(v) for k, v in obj.items()}
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return obj
        
    def normaliza_texto(self, texto):
        if not isinstance(texto, str):
            return ""
        texto = texto.lower()
        texto = texto.replace(" ", "_")
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
        return texto

    def get_hoteles(self):
        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.Location.UBICACIONES,
                f"answers.{self.f['tipo_ubicacion']}": "hotel",
            }},
            {'$project': {
                '_id': 0,
                'nombre_hotel': f"$answers.{self.Location.f['location']}",
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))
        # TODO
        # Temp se quita mcallen de la lista porque no es un hotel valido
        result = [h for h in result if h.get('nombre_hotel', '').strip().upper() != 'MCALLEN']
        
        hoteles = {
            'hoteles': result
        }

        return hoteles
    
    def get_cantidad_si_y_no(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list},
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'paresAnswers': { '$objectToArray': "$answers" }
            }},
            {'$unwind': "$paresAnswers"},
            {'$match': {
                "paresAnswers.v": { '$in': [ "sí", "no" ] }
            }},
            {'$group': {
                '_id': "$paresAnswers.v",
                'total': { '$sum': 1 }
            }},
            {'$project': {
                '_id': 0,
                'respuesta': '$_id',
                'total': 1
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))
        
        return result
    
    def get_cantidad_habitaciones(self, ubicaciones_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
            f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area']}": "Habitación",
        }

        if isinstance(ubicaciones_list, list) and len(ubicaciones_list) > 1:
            match_query.update({
               f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": {"$in": ubicaciones_list}
            })
        elif isinstance(ubicaciones_list, list) and len(ubicaciones_list) == 1:
            match_query.update({
               f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.Location.f['location']}": self.unlist(ubicaciones_list)
            })

        query = [
            {'$match': match_query},
            {
                '$count': "totalHabitaciones"
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))
        result = self.unlist(result)

        return result

    def get_cantidad_inspecciones_y_remodeladas(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list}
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list)
            })

        query = [
            {'$match': match_query},
            {'$project': {
                'status_completada': {
                    '$cond': [
                        { '$eq': [f"$answers.{self.f['status_auditoria']}", "completada"] },
                        1,
                        0
                    ]
                },
                'habitacion_remodelada_si': {
                    '$cond': [
                        { '$eq': [f"$answers.{self.f['habitacion_remodelada']}", "sí"] },
                        1,
                        0
                    ]
                }
            }},
            {
                '$group': {
                    '_id': None,
                    'total_inspecciones_completadas': { '$sum': "$status_completada" },
                    'total_habitaciones_remodeladas': { '$sum': "$habitacion_remodelada_si" }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'total_inspecciones_completadas': 1,
                    'total_habitaciones_remodeladas': 1
                }
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))
        result = self.unlist(result)

        return result
    
    def resumen_cuatrimestres(self, calificaciones_dict):
        calificaciones = calificaciones_dict.get('cuatrimestres', [])
        if not calificaciones:
            return {
                'max_global': None,
                'min_global': None,
                'promedio_global': None
            }
    
        max_global = max(item['max'] for item in calificaciones)
        min_global = min(item['min'] for item in calificaciones)
        promedio_global = round(sum(item['promedio'] for item in calificaciones) / len(calificaciones), 2)
    
        return {
            'max_global': max_global,
            'min_global': min_global,
            'promedio_global': promedio_global
        }

    def get_calificaciones(self, forms_id_list=[], anio=None, cuatrimestres=None):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list},
            })
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {
                "$lookup": {
                    "from": "voucher",
                    "let": { "vid": { "$toObjectId": "$voucher_id" } },
                    "pipeline": [
                        { "$match": { "$expr": { "$eq": [ "$_id", "$$vid" ] } } },
                        { "$project": { "_id": 0, "max_points": "$grading.max_points" } }
                    ],
                    "as": "voucherInfo"
                }
            },
            {
                "$unwind": {
                    "path": "$voucherInfo",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$match": {
                    "points": { "$type": "double" }
                }
            },
            {
                "$addFields": {
                    "calificacion": {
                        "$multiply": [
                            { "$divide": [ "$points", "$voucherInfo.max_points" ] },
                            100
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_fecha": {
                        "$cond": [
                            {
                                "$in": [
                                    { "$type": "$start_date" },
                                    [ "double", "int", "long", "decimal" ]
                                ]
                            },
                            { "$toDate": { "$multiply": [ "$start_date", 1000 ] } },
                            "$start_date"
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_anio": { "$year":  "$_fecha" },
                    "_mes":  { "$month": "$_fecha" }
                }
            },
            {
                "$addFields": {
                    "_cuatrimestre": {
                        "$ceil": { "$divide": [ "$_mes", 4 ] }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "anio":         "$_anio",
                        "cuatrimestre": "$_cuatrimestre"
                    },
                    "maxCalif": { "$max": "$calificacion" },
                    "minCalif": { "$min": "$calificacion" },
                    "avgCalif": { "$avg": "$calificacion" }
                }
            },
            {
                "$project": {
                    "_id":          0,
                    "anio":         "$_id.anio",
                    "cuatrimestre": "$_id.cuatrimestre",
                    "max":          { "$round": [ "$maxCalif", 2 ] },
                    "min":          { "$round": [ "$minCalif", 2 ] },
                    "promedio":     { "$round": [ "$avgCalif", 2 ] }
                }
            }
        ]

        match_filter = {}
        if anio is not None:
            match_filter["anio"] = anio
        if cuatrimestres:
            match_filter["cuatrimestre"] = {"$in": cuatrimestres}
        if match_filter:
            query.append({"$match": match_filter})

        query.append({
            "$sort": {
                "anio": 1,
                "cuatrimestre": 1
            }
        })

        result = self.format_cr(self.cr.aggregate(query))

        calificaciones = {
            'cuatrimestres': result,
        }

        resumen = self.resumen_cuatrimestres(calificaciones)
        calificaciones['resumen'] = resumen

        return calificaciones

    def porcentaje_propiedades_inspeccionadas(self, total_habitaciones, total_inspecciones):
        if total_habitaciones == 0:
            return 0.0
    
        porcentaje = (total_inspecciones / total_habitaciones) * 100
        return round(porcentaje, 2)

    def get_forms_id_list(self, hoteles=None):
        if not hoteles:
            return list(self.form_ids.values())

        hoteles_normalizados = [
            hotel.lower().replace(' ', '_') for hotel in hoteles
        ]

        ids = [
            self.form_ids[nombre]
            for nombre in hoteles_normalizados
            if nombre in self.form_ids
        ]

        return ids

    def get_cuatrimestres_by_hotel(self, hoteles=[], anio=None, cuatrimestres=None):
        forms_id_list = self.get_forms_id_list(hoteles)
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            }) # type: ignore
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {
                "$lookup": {
                    "from": "voucher",
                    "let": { "vid": { "$toObjectId": "$voucher_id" } },
                    "pipeline": [
                        { "$match": { "$expr": { "$eq": [ "$_id", "$$vid" ] } } },
                        { "$project": { "_id": 0, "max_points": "$grading.max_points" } }
                    ],
                    "as": "voucherInfo"
                }
            },
            {
                "$unwind": {
                    "path": "$voucherInfo",
                    "preserveNullAndEmptyArrays": False
                }
            },
            {
                "$match": {
                    "points": { "$type": "double" }
                }
            },
            {
                "$addFields": {
                    "calificacion": {
                        "$multiply": [
                            { "$divide": [ "$points", "$voucherInfo.max_points" ] },
                            100
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_fecha": {
                        "$cond": [
                            {
                                "$in": [
                                    { "$type": "$start_date" },
                                    [ "double", "int", "long", "decimal" ]
                                ]
                            },
                            { "$toDate": { "$multiply": [ "$start_date", 1000 ] } },
                            "$start_date"
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "_anio": { "$year":  "$_fecha" },
                    "_mes":  { "$month": "$_fecha" }
                }
            },
            {
                "$addFields": {
                    "_cuatrimestre": {
                        "$ceil": { "$divide": [ "$_mes", 4 ] }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "form_id": "$form_id",
                        "anio": "$_anio",
                        "cuatrimestre": "$_cuatrimestre"
                    },
                    "maxCalif": { "$max": "$calificacion" },
                    "minCalif": { "$min": "$calificacion" },
                    "avgCalif": { "$avg": "$calificacion" }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "form_id": "$_id.form_id",
                    "anio": "$_id.anio",
                    "cuatrimestre": "$_id.cuatrimestre",
                    "max": { "$round": [ "$maxCalif", 2 ] },
                    "min": { "$round": [ "$minCalif", 2 ] },
                    "promedio": { "$round": [ "$avgCalif", 2 ] }
                }
            }
        ]

        match_filter = {}
        if anio is not None:
            match_filter["anio"] = anio
        if cuatrimestres:
            match_filter["cuatrimestre"] = {"$in": cuatrimestres}
        if match_filter:
            query.append({"$match": match_filter})

        query.append({
            "$sort": {
                "form_id": 1,
                "anio": 1,
                "cuatrimestre": 1
            }
        })

        result = self.format_cr(self.cr.aggregate(query))

        form_id_to_hotel = {v: k for k, v in self.form_ids.items()}
        for item in result:
            item["hotel"] = form_id_to_hotel.get(item["form_id"], item["form_id"])

        hoteles_list = []
        agrupados = {}

        for item in result:
            hotel = item["hotel"]
            if hotel not in agrupados:
                agrupados[hotel] = []
            agrupados[hotel].append({
                "anio": item["anio"],
                "cuatrimestre": item["cuatrimestre"],
                "max": item["max"],
                "min": item["min"],
                "promedio": item["promedio"]
            })

        for hotel, cuatrimestres in agrupados.items():
            hoteles_list.append({
                "hotel": hotel,
                "cuatrimestres": cuatrimestres
            })

        return hoteles_list

    def get_fallas(self, forms_id_list=[], anio=None, cuatrimestres=None):
        match_query = {
            "deleted_at": {"$exists": False},
        }
    
        if len(forms_id_list) > 1:
            match_query["form_id"] = {"$in": forms_id_list}
        else:
            match_query["form_id"] = self.unlist(forms_id_list)
    
        group_fields = {"_id": "$form_id"}
        total_fields = {"_id": None}
        for nombre_falla, id_falla in self.fallas_hotel.items():
            group_fields[nombre_falla] = { # type: ignore
                "$sum": {
                    "$cond": [
                        {"$eq": [f"$answers.{id_falla}", "sí"]},
                        1,
                        0
                    ]
                }
            }
            total_fields[nombre_falla] = { # type: ignore
                "$sum": {
                    "$cond": [
                        {"$eq": [f"$answers.{id_falla}", "sí"]},
                        1,
                        0
                    ]
                }
            }

        query_por_hotel = [
            {"$match": match_query},
            {"$group": group_fields}
        ]

        result_por_hotel = self.format_cr(self.cr.aggregate(query_por_hotel))
    
        form_id_to_hotel = {str(v): k for k, v in self.form_ids.items()}
        for item in result_por_hotel:
            item["hotel"] = form_id_to_hotel.get(str(item["_id"]), item["_id"])
            del item["_id"]
    
        query_totales = [
            {"$match": match_query},
            {"$group": total_fields}
        ]
        result_totales = self.format_cr(self.cr.aggregate(query_totales))
        totales = result_totales[0] if result_totales else {}
        if "_id" in totales:
            del totales["_id"]

        totales_list = [{"falla": k, "total": v} for k, v in totales.items()]
        totales_list.sort(key=lambda x: x["total"], reverse=True)

        return {
            "por_hotel": result_por_hotel,
            "totales": totales_list
        }

    def get_habitaciones_by_hotel(self, hotel_name, fallas=None):
        hotel_name_list = [hotel_name.lower().replace(' ', '_')]
        form_id = self.get_forms_id_list(hotel_name_list)
        form_id = self.unlist(form_id)

        if hotel_name in self.hotel_name_abreviatura:
            hotel_name = self.hotel_name_abreviatura[hotel_name]

        query = [
            {'$match': {
                "deleted_at": {"$exists": False},
                "form_id": self.Location.AREAS_DE_LAS_UBICACIONES,
                f"answers.{self.Location.UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}": hotel_name,
                f"answers.{self.Location.TIPO_AREA_OBJ_ID}.{self.f['tipo_area_habitacion']}": "Habitación",
            }},
            {'$project': {
                '_id': 0,
                'nombre_area_habitacion': f"$answers.{self.f['nombre_area_habitacion']}",
                'numero_habitacion': f"$answers.{self.f['numero_habitacion']}",
            }},
            {
            '$lookup': {
                'from': 'form_answer',
                'let': {
                    'nombre_hab': '$nombre_area_habitacion'
                },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                {'$eq': ['$form_id', form_id]},
                                {'$eq': [f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}", "$$nombre_hab"]}
                                ]
                            }
                        }
                    },
                    {
                        '$sort': {'created_at': -1}
                    },
                    {
                        '$limit': 1
                    },
                    {
                        '$project': {
                            '_id': 1
                        }
                    }
                ],
                'as': 'inspeccion'
            }
            },
            {
            '$addFields': {
                'inspeccion_id': {
                    '$cond': [
                        {'$gt': [{'$size': '$inspeccion'}, 0]},
                        {'$arrayElemAt': ['$inspeccion._id', 0]},
                        None
                    ]
                }
            }
            },
            {
                '$project': {
                    'nombre_area_habitacion': 1,
                    'numero_habitacion': 1,
                    'inspeccion_id': 1
                }
            }
        ]

        habitaciones = self.format_cr(self.cr.aggregate(query))
        habitaciones_id = [hab.get('inspeccion_id') for hab in habitaciones]
        query = {"_id": {"$in": habitaciones_id}}
        res = self.cr_inspeccion.find(query)

        habitaciones_id = [hab.get('inspeccion_id') for hab in habitaciones if hab.get('inspeccion_id') is not None]
        if habitaciones_id:
            query = {"_id": {"$in": habitaciones_id}}
            res = self.cr_inspeccion.find(query)
            inspecciones = list(res)
            inspecciones_dict = {x['_id']: x for x in inspecciones}
        else:
            inspecciones_dict = {}

        for hab in habitaciones:
            inspeccion_id = hab.get('inspeccion_id')
            if inspeccion_id and inspeccion_id in inspecciones_dict:
                hab['inspeccion_habitacion'] = inspecciones_dict[inspeccion_id]
            else:
                hab['inspeccion_habitacion'] = None

        habitaciones = self.normalize_types(habitaciones)

        if fallas:
            fallas_normalizadas = set(self.normaliza_texto(f) for f in fallas)
            for hab in habitaciones:
                inspeccion = hab.get('inspeccion_habitacion') # type: ignore
                if inspeccion and 'field_label' in inspeccion:
                    labels = inspeccion['field_label'].values() # type: ignore
                    labels_normalizadas = set(self.normaliza_texto(l) for l in labels)
                    if not any(falla in labels_normalizadas for falla in fallas_normalizadas):
                        hab['inspeccion_habitacion'] = None # type: ignore

        return {
            'habitaciones': habitaciones,
        }
    
    def get_room_data(self, hotel_name, room_id):
        hotel_name_list = [hotel_name.lower().replace(' ', '_')]
        form_id = self.get_forms_id_list(hotel_name_list)
        form_id = self.unlist(form_id)

        query = [
            {
                '$match': {
                    "deleted_at": {"$exists": False},
                    "form_id": form_id,
                    f"answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}": room_id,
                }
            },
            {
              '$sort': {
                   'created_at': -1
                }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    '_id': 1,
                    'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                    'habitacion_remodelada': f"$answers.{self.f['habitacion_remodelada']}",
                    'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                    'created_by_name': "$created_by_name",
                    'created_at': "$created_at",
                    'updated_at': "$updated_at",
                }
            },
            {
                '$lookup': {
                    'from': 'inspeccion_hoteleria',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'inspeccion'
                }
            },
            {'$project': {
                    '_id': 1,
                    'habitacion': '$habitacion',
                    'habitacion_remodelada': '$habitacion_remodelada',
                    'nombre_camarista': '$nombre_camarista',
                    'created_by_name': '$created_by_name',
                    'created_at': '$created_at',
                    'updated_at': '$updated_at',
                    'inspeccion': {'$first': '$inspeccion'}
                }
            },
        ]
        result = self.cr.aggregate(query)
        x = {}
        for x in result:
            x['_id'] = str(x['_id'])
            x['inspeccion'].pop('_id', None)
            x['created_at'] = self.get_date_str(x['created_at'])
            x['updated_at'] = self.get_date_str(x['updated_at'])
            if x['inspeccion'].get('created_at'):
                x['inspeccion'].pop('created_at', None)
        if not x:
            return {"mensaje": "No hay inspección para esta habitación"}
        return x

    def get_table_habitaciones_inspeccionadas(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }

        if len(forms_id_list) > 1:
            match_query["form_id"] = {"$in": forms_id_list}
        else:
            match_query["form_id"] = self.unlist(forms_id_list)

        # Prepara los campos de fallas para el project
        fallas_project = {}
        for nombre_falla, id_falla in self.fallas_hotel.items():
            fallas_project[nombre_falla] = f"$answers.{id_falla}"

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                'created_at': '$created_at',
                **fallas_project
            }},
            {
                '$lookup': {
                    'from': 'inspeccion_hoteleria',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'inspeccion'
                }
            },
            {
                '$unwind': {
                    'path': '$inspeccion',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$addFields': {
                    'grade': '$inspeccion.grade'
                }
            }
        ]

        result = self.format_cr(self.cr.aggregate(query))

        output = []
        for doc in result:
            if not doc.get('hotel'):
                continue
            total_fallas = 0
            for nombre_falla in self.fallas_hotel.keys():
                if doc.get(nombre_falla) == "sí":
                    total_fallas += 1
            created_at = doc.get('created_at')
            if isinstance(created_at, datetime.datetime):
                created_at = created_at.strftime('%d/%m/%Y')
            output.append({
                'habitacion': doc.get('habitacion'),
                'hotel': doc.get('hotel'),
                'nombre_camarista': doc.get('nombre_camarista'),
                'created_at': created_at,
                'grade': doc.get('grade'),
                'total_fallas': total_fallas
            })

        return output
    
    def get_mejor_y_peor_habitacion(self, forms_id_list=[]):
        match_query = {
            "deleted_at": {"$exists": False},
        }
    
        if len(forms_id_list) > 1:
            match_query["form_id"] = {"$in": forms_id_list}
        else:
            match_query["form_id"] = self.unlist(forms_id_list)
    
        fallas_project = {}
        for nombre_falla, id_falla in self.fallas_hotel.items():
            fallas_project[nombre_falla] = f"$answers.{id_falla}"
    
        pipeline = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                'nombre_camarista': f"$answers.{self.f['nombre_camarista']}",
                'created_at': '$created_at',
                **fallas_project
            }},
            {
                '$lookup': {
                    'from': 'inspeccion_hoteleria',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'inspeccion'
                }
            },
            {
                '$unwind': {
                    'path': '$inspeccion',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$addFields': {
                    'grade': '$inspeccion.grade'
                }
            },
            # Calcula el total de fallas en cada inspección
            {
                '$addFields': {
                    'total_fallas': {
                        '$size': {
                            '$filter': {
                                'input': [f"${k}" for k in self.fallas_hotel.keys()],
                                'as': 'falla',
                                'cond': { '$eq': ['$$falla', 'sí'] }
                            }
                        }
                    }
                }
            },
            # Agrupa por habitación y hotel
            {
                '$group': {
                    '_id': {
                        'habitacion': '$habitacion',
                        'hotel': '$hotel'
                    },
                    'grades': { '$push': '$grade' },
                    'fallas': { '$push': '$total_fallas' },
                    'total_inspecciones': { '$sum': 1 }
                }
            }
        ]
    
        result = self.format_cr(self.cr.aggregate(pipeline))
    
        mejor = None
        peor = None
        for hab in result:
            grades = [g for g in hab.get('grades', []) if g is not None]
            fallas = hab.get('fallas', [])
            promedio_grade = sum(grades) / len(grades) if grades else 0
            max_fallas = max(fallas) if fallas else 0

            # Usa los campos directos, o busca en _id si no existen
            habitacion = hab.get('habitacion') or (hab.get('_id', {}) or {}).get('habitacion')
            hotel = hab.get('hotel') or (hab.get('_id', {}) or {}).get('hotel')
            total_inspecciones = hab.get('total_inspecciones', 0)

            if not habitacion or not hotel:
                continue

            # Mejor: mayor promedio de grade
            if not mejor or promedio_grade > mejor['grade']:
                mejor = {
                    'habitacion': habitacion,
                    'hotel': hotel,
                    'total_inspecciones': total_inspecciones,
                    'grade': round(promedio_grade, 2),
                    'total_fallas': min(fallas) if fallas else 0
                }
            # Peor: mayor cantidad de fallas
            if not peor or max_fallas > peor['total_fallas']:
                peor = {
                    'habitacion': habitacion,
                    'hotel': hotel,
                    'total_inspecciones': total_inspecciones,
                    'grade': round(min(grades), 2) if grades else 0,
                    'total_fallas': max_fallas
                }
    
        return {
            'mejor_habitacion': mejor,
            'habitacion_mas_fallas': peor
        }
    
    def get_graph_radar(self, forms_id_list=None):
        query = {}
        projection = {"_id": 1, "sections": 1}
        res = list(self.cr_inspeccion.find(query, projection))
        list_of_ids = [r['_id'] for r in res]

        match_query = {
            "deleted_at": {"$exists": False},
            "_id": {"$in": list_of_ids}
        }

        if len(forms_id_list) > 1: # type: ignore
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            }) # type: ignore
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))

        hotel_by_id = {str(item['_id']): item['hotel'] for item in result}
        for r in res:
            r['_id'] = str(r['_id'])
            r_id = r['_id']
            r['hotel'] = hotel_by_id.get(r_id, None)

        return {
            'radar_data': res
        }

    def get_fotografias(self, forms_id_list=None):
        query = {}
        projection = {"_id": 1, "media": 1}
        res = list(self.cr_inspeccion.find(query, projection))
        list_of_ids = [r['_id'] for r in res]

        match_query = {
            "deleted_at": {"$exists": False},
            "_id": {"$in": list_of_ids}
        }

        if len(forms_id_list) > 1: # type: ignore
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            }) # type: ignore
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))

        hotel_by_id = {str(item['_id']): item['hotel'] for item in result}
        for r in res:
            r['_id'] = str(r['_id'])
            r_id = r['_id']
            r['hotel'] = hotel_by_id.get(r_id, None)
            # Formatea media: solo deja el id y los file_url
            media = r.get('media', {})
            new_media = {}
            for key, files in media.items():
                # Solo file_url en cada diccionario
                new_media[key] = [{'file_url': f['file_url']} for f in files if 'file_url' in f]
            r['media'] = new_media

        return {
            'hoteles_fotografias': res
        }
    
    def get_comentarios(self, forms_id_list=None):
        query = {}
        res = list(self.cr_inspeccion.find(query))
        print('res', res[3])
        list_of_ids = [r['_id'] for r in res]

        match_query = {
            "deleted_at": {"$exists": False},
            "_id": {"$in": list_of_ids}
        }

        if len(forms_id_list) > 1: # type: ignore
            match_query.update({
                "form_id": {"$in": forms_id_list}, # type: ignore
            }) # type: ignore
        else:
            match_query.update({
                "form_id": self.unlist(forms_id_list),
            })

        query = [
            {'$match': match_query},
            {'$project': {
                '_id': 1,
                'hotel': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['ubicacion_nombre']}",
                'habitacion': f"$answers.{self.Location.AREAS_DE_LAS_UBICACIONES_CAT_OBJ_ID}.{self.f['nombre_area_habitacion']}",
            }},
        ]

        result = self.format_cr(self.cr.aggregate(query))

        hotel_by_id = {str(item['_id']): item['hotel'] for item in result}
        info_by_id = {
            str(item['_id']): {
                'hotel': item.get('hotel'),
                'habitacion': item.get('habitacion')
            }
            for item in result
        }
        for r in res:
            r['_id'] = str(r['_id'])
            info = info_by_id.get(r['_id'], {})
            r['hotel'] = info.get('hotel')
            r['habitacion'] = info.get('habitacion')
            r.pop('created_at', None)  # Elimina created_at si existe
            # Formatea media: solo deja el id y los file_url
            media = r.get('media', {})
            new_media = {}
            for key, files in media.items():
                # Solo file_url en cada diccionario
                new_media[key] = [{'file_url': f['file_url']} for f in files if 'file_url' in f]
            r['media'] = new_media

        return {
            'hoteles_comentarios': res
        }

    def get_report(self, anio=None, cuatrimestres=None, hoteles=[]):
        # Normaliza los nombres de hoteles usando las abreviaturas si corresponde
        if hoteles:
            hoteles_actualizados = []
            for hotel in hoteles:
                encontrado = False
                hotel_norm = hotel.lower().replace(' ', '_')
                for k, v in self.hotel_name_abreviatura.items():
                    v_norm = v.lower().replace(' ', '_')
                    k_norm = k.lower().replace(' ', '_')
                    if v_norm == hotel_norm:
                        hoteles_actualizados.append(k_norm)
                        encontrado = True
                        break
                if not encontrado:
                    hoteles_actualizados.append(hotel_norm)
            hoteles = hoteles_actualizados

        forms_id_list = self.get_forms_id_list(hoteles)
        print("forms_id_list", forms_id_list)

        cantidad_si_y_no = self.get_cantidad_si_y_no(forms_id_list=forms_id_list)

        total_habitaciones = self.get_cantidad_habitaciones(ubicaciones_list=hoteles)

        total_inspecciones_y_remodeladas = self.get_cantidad_inspecciones_y_remodeladas(forms_id_list=forms_id_list)

        calificaciones = self.get_calificaciones(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)

        propiedades_inspeccionadas = self.porcentaje_propiedades_inspeccionadas(
            total_habitaciones.get('totalHabitaciones', 0) if total_habitaciones else 0,
            total_inspecciones_y_remodeladas.get('total_inspecciones_completadas', 0) if total_inspecciones_y_remodeladas else 0
        )
        
        calificacion_x_hotel_grafica = self.get_cuatrimestres_by_hotel(hoteles=hoteles, anio=anio, cuatrimestres=cuatrimestres)

        fallas = self.get_fallas(forms_id_list=forms_id_list, anio=anio, cuatrimestres=cuatrimestres)

        habitaciones_inspeccionadas = self.get_table_habitaciones_inspeccionadas(forms_id_list=forms_id_list)

        mejor_y_peor_habitacion = self.get_mejor_y_peor_habitacion(forms_id_list=forms_id_list)

        graph_radar = self.get_graph_radar(forms_id_list=forms_id_list)

        hoteles_fotografias = self.get_fotografias(forms_id_list=forms_id_list)

        hoteles_comentarios = self.get_comentarios(forms_id_list=forms_id_list)

        report_data = {
            'cantidad_si_y_no': cantidad_si_y_no,
            'total_habitaciones': total_habitaciones,
            'total_inspecciones_y_remodeladas': total_inspecciones_y_remodeladas,
            'calificaciones': calificaciones,
            'porcentaje_propiedades_inspeccionadas': propiedades_inspeccionadas,
            'calificacion_x_hotel_grafica': calificacion_x_hotel_grafica,
            'fallas': fallas,
            'table_habitaciones_inspeccionadas': habitaciones_inspeccionadas,
            'mejor_y_peor_habitacion': mejor_y_peor_habitacion,
            'graph_radar': graph_radar,
            'hoteles_fotografias': hoteles_fotografias,
            'hoteles_comentarios': hoteles_comentarios,
        }

        return report_data

    def get_pdf(self, record_id, template_id=131835, name_pdf='Inspeccion de Habitacion'):
        return self.lkf_api.get_pdf_record(record_id, template_id=template_id, name_pdf=name_pdf, send_url=True)

if __name__ == '__main__':
    module_obj = Inspeccion_Hoteleria(settings, sys_argv=sys.argv, use_api=True)
    module_obj.console_run()
    data = module_obj.data.get('data', {})
    option = data.get('option', 'get_report')
    anio = data.get('anio', None)
    cuatrimestres = data.get('cuatrimestres', None)
    hoteles = data.get('hoteles', [])
    hotel_name = data.get('hotel_name', 'CROWNE PLAZA MTY')
    room_id = data.get('room_id', 'Habitación 326')
    fallas = data.get('fallas', ['plafon_fuera_de_la_habitacion'])
    record_id = data.get('record_id', None)

    if option == 'get_hoteles':
        response = module_obj.get_hoteles()
    elif option == 'get_report':
        response = module_obj.get_report(anio=anio, cuatrimestres=cuatrimestres, hoteles=hoteles)
    elif option == 'get_habitaciones_by_hotel':
        response = module_obj.get_habitaciones_by_hotel(hotel_name=hotel_name, fallas=fallas)
    elif option == 'get_room_data':
        response = module_obj.get_room_data(hotel_name=hotel_name, room_id=room_id)
    elif option == 'get_room_pdf':
        response = module_obj.get_pdf(record_id=record_id)

    # print('response=', response)
    print(simplejson.dumps(response, indent=3))
    module_obj.HttpResponse({"data": response})