# -*- coding: utf-8 -*-
import sys, simplejson
from produccion_pci_utils import Produccion_PCI
from account_settings import *

class ValidarXLSDescuentos( Produccion_PCI ):
    """docstring for ValidarXLSDescuentos"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.users_only_status_espera = [29300]

    def normalizar_descripcion(self, desc):
        desc = desc.lower().strip().replace("($)", "")
        return {
            "nómina": "nomina",
            "préstamo": "prestamo",
            "vehículo": "vehiculo"
        }.get(desc, desc)

    def validar_xls(self, file_url):
        header, records_descuentos = self.upfile.get_file_to_upload(file_url=file_url, form_id=self.form_id)
        if not records_descuentos:
            return {}
        
        dict_emails_descuentos, error_descuentos, dict_descripciones_by_connection = {}, [], {}
        descuentos_aceptados = set(p_utils.map_descuentos_by_xls.keys()) | {"bono"}
        dict_subtotales_default = { str(rec_des[1]).lower().strip(): rec_des[6] for rec_des in records_descuentos if rec_des[6] }
        
        for pos_des, rec_des in enumerate(records_descuentos):
            email_rec = str(rec_des[1]).lower().strip()
            division_rec = str(rec_des[2]).lower().strip()
            tecnologia_rec = str(rec_des[3]).lower().strip()
            subtotal_default = rec_des[6]
            num_row = pos_des + 1

            if division_rec == "psr" or tecnologia_rec == "psr":
                tecnologia_rec = "psr"
            else:
                if division_rec not in ["metro", "sur", "norte", "occidente"]:
                    error_descuentos.append(f"RENGLON {num_row}: División debe ser metro, sur, norte u occidente")
                    continue
                if tecnologia_rec not in ["fibra", "cobre"]:
                    error_descuentos.append(f"RENGLON {num_row}: Tecnología debe ser fibra o cobre")
                    continue
            
            if rec_des[0] and not isinstance(rec_des[0], (int, float)):
                error_descuentos.append(f"RENGLON {num_row}: El descuento debe ser de tipo número")
                continue
            if rec_des[4] and not isinstance(rec_des[4], (int, float)):
                error_descuentos.append(f"RENGLON {num_row}: El porcentaje de descuento debe ser de tipo número")
                continue
            if subtotal_default and not isinstance(subtotal_default, (int, float)):
                error_descuentos.append(f"RENGLON {num_row}: El subtotal de la OC debe ser de tipo número")
                continue
            
            descripcion_descuento = rec_des[5]
            if not descripcion_descuento and not subtotal_default:
                error_descuentos.append(f"RENGLON {num_row}: Se requiere la Descripción del descuento")
                continue
            
            dict_descripciones_by_connection.setdefault(email_rec, {}).setdefault(tecnologia_rec, {'descripciones': [], 'montos': 0, 'porcentajes': 0})
            if descripcion_descuento:
                descripcion_descuento = self.normalizar_descripcion(descripcion_descuento)
                is_bono = descripcion_descuento == "bono"

                if not any( d in descripcion_descuento for d in descuentos_aceptados ):
                    error_descuentos.append(f"RENGLON {num_row}: Valor no aceptado en la Descripción del descuento")
                    continue

                # Bono se aplica como suma
                # Aqui se requiere la columna de Monto, porque no se aplica % ni subtotal
                if is_bono and not rec_des[0]:
                    error_descuentos.append(f"RENGLON {num_row}: Se requiere Monto para aplicar el Bono")
                    continue
                
                # NO pueden haber descripciones repetidas para un solo contratista
                if descripcion_descuento in dict_descripciones_by_connection[email_rec][tecnologia_rec]['descripciones']:
                    error_descuentos.append( f"RENGLON {num_row}: Ya hay un descuento con la misma descripcion" )
                    continue
                dict_descripciones_by_connection[email_rec][tecnologia_rec]['descripciones'].append(descripcion_descuento)
            dict_descripciones_by_connection[email_rec][tecnologia_rec]['montos'] += rec_des[0] or 0
            dict_descripciones_by_connection[email_rec][tecnologia_rec]['porcentajes'] += rec_des[4] or 0

        '''
        # Validaciones sobre el subtotal default y los porcentajes y descuentos aplicados
        '''
        for email_conexion, dict_tecnologias in dict_descripciones_by_connection.items():
            for tecnologia_descuento, info_descuento in dict_tecnologias.items():
                # Porcentaje no puede ser mayor al 100 %
                if info_descuento.get('porcentajes', 0) > 100:
                    error_descuentos.append(f"{email_conexion} el porcentaje no debe ser mayor al 100%")
                    continue
                
                # si se define un subtotal y hay tambien montos de descuentos pues no deberan ser > al subtotal definido
                if dict_subtotales_default.get(email_conexion):
                    if info_descuento.get('montos',0) > dict_subtotales_default[email_conexion]:
                        error_descuentos.append(f"{email_conexion} la suma de los descuentos es mayor al subtotal definido")
                        continue

        if error_descuentos:
            raise Exception( simplejson.dumps( {
                "f2362800a0100000000000b2":{
                    "msg": [ self.list_to_str(error_descuentos) ], "label": "Descuentos", "error":[]
                }
            } ) )

    def validate_xls_descuentos(self):
        """
        # Revisiones de las personas marcadas en la carga
        # 1 .- Cuando cree un registro forzar para que siempre se ponga estatus En espera de proceso de orden de compra
        """
        force_status = user_id in self.users_only_status_espera
        if force_status:
            current_record['answers']['5f10d2efbcfe0371cb2fbd39'] = 'en_espera_de_proceso_orden_de_compra'

        if current_record['answers'].get('5f10d2efbcfe0371cb2fbd39', '') == 'en_espera_de_proceso_orden_de_compra':
            file_url = current_record['answers'].get('f2362800a0100000000000b2', {}).get('file_url')
            if file_url:
                self.validar_xls(file_url)
        
        if force_status:
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': current_record['answers']
            }))
        
if __name__ == '__main__':
    lkf_obj = ValidarXLSDescuentos( settings, sys_argv=sys.argv )
    lkf_obj.console_run()
    
    current_record = lkf_obj.current_record
    user_id = lkf_obj.user.get('user_id')

    print(f"+++ usuario = {user_id}")

    from pci_base_utils import PCI_Utils
    p_utils = PCI_Utils()

    lkf_obj.validate_xls_descuentos()