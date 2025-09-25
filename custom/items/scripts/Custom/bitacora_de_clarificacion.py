# -*- coding: utf-8 -*-
import sys, simplejson, re
from fractions import Fraction
from custom_utils import Custom
from account_settings import *

class Custom(Custom):
    """docstring for Custom"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

    def prepare_data_to_group(self, var_data, value, str_rango_referencia, value_as_str):
        """
        Se procesan los datos del campo encontrado como fuera de rango

        Args:
            var_data (dict): Informacion del campo de la variable
            value (float): Valor obtenido del answers del registro
            str_rango_referencia (str): Texto con la referencia de minimos y maximos

        Return:
            Diccionario con valores que se iran integrando al grupo repetitivo de resultados
        """
        # if isinstance(value, Fraction):
        #     value = float(value)
        
        return {
            self.f['fuera_rango_variable']: var_data.get('label'),
            self.f['fuera_rango_valor_obtenido']: str(value) if value_as_str else value,
            self.f['fuera_rango_rango_referencia']: str_rango_referencia
        }

    def is_fracciones(self, val):
        """
        Se revisa si el valor es texto y aplica para fracciones
        """
        return isinstance(val, str) and "/" in val

    def get_partes_fraccion(self, val):
        """
        Desarma la fraccion para obtener el numerador y denominador
        """
        def convert_int(v):
            try:
                return int( v.replace(' ', '') )
            except:
                return None

        val_split = val.split("/")

        return convert_int( val_split[0] ), convert_int( val_split[1] )

    def process_variables(self, dict_variables, data_config, answers_to_get_values, value_as_str, processing_catalog_values=False):
        """
        Procesa el diccionario de variables y las compara con las configuraciones que tiene en el catalogo
        
        Args:
            dict_variables (dict): Diccionario de las variables a considerar
            data_config (dict): Configuraciones en el catalogo
            answers_to_get_values (dict): answers con la informacion que captura el usuario y de donde se obtendrán los valores

        Return:
            lista de sets que se van a integrar al grupo repetitivo de resultados
        """
        list_items_to_group = []
        for var_field_id, var_data in dict_variables.items():
            # Ultimos dígitos del id despues de aaaaa ó ccaaa
            last_digits = var_field_id[5:]

            # Id de los campos maximos y minimos
            field_min = f"bbbbb{last_digits}"
            field_max = f"ddddd{last_digits}"

            # Se obtiene el máximo y mínimo de la variable procesada
            min_var = data_config.get(field_min)
            max_var = data_config.get(field_max)

            # se obtiene el valor en el answers
            value_var = self.unlist( answers_to_get_values.get(var_field_id) )

            # Si se están procesando los valors de los campos de Catalogo hay que revisar que venga la leyenda "Ultimo Valor" para considerarlo
            if processing_catalog_values:
                # Hago el if value_var por casos donde el valor viene como []
                if not value_var or "ultimo valor" not in value_var.lower():
                    continue
                value_var = value_var.split(':')[1].strip()
            
            # Si no hay informacion capturada por el usuario, o no hay minimos y maximos definidos se continúa con el siguiente campo
            if not value_var or not any( [min_var, max_var] ):
                continue

            # Hay algunos casos excepcionales donde se manejan fracciones
            if self.is_fracciones(min_var) or self.is_fracciones(max_var):
                
                numerador_min, denominador_min = self.get_partes_fraccion( min_var )
                if not all( [numerador_min, denominador_min] ):
                    raise Exception( simplejson.dumps({
                        'folio': {'msg': [f'Error al convertir a fracciones el mínimo del campo {var_field_id}'], 'label': 'Folio', 'error': []}
                    }) )

                numerador_max, denominador_max = self.get_partes_fraccion( max_var )
                if not all( [numerador_max, denominador_max] ):
                    raise Exception( simplejson.dumps({
                        'folio': {'msg': [f'Error al convertir a fracciones el máximo del campo {var_field_id}'], 'label': 'Folio', 'error': []}
                    }) )

                if not self.is_fracciones(value_var):
                    raise Exception( simplejson.dumps({
                        var_field_id: {'msg': [f'El valor debe ser especificado en Francciones'], 'label': var_data.get('label'), 'error': []}
                    }) )

                numerador_val, denominador_val = self.get_partes_fraccion( value_var )
                if not all( [numerador_val, denominador_val] ):
                    raise Exception( simplejson.dumps({
                        var_field_id: {'msg': [f'Error al convertir a fracciones'], 'label': var_data.get('label'), 'error': []}
                    }) )

                min_var = Fraction( numerador_min, denominador_min )
                max_var = Fraction( numerador_max, denominador_max )
                value_var = Fraction( numerador_val, denominador_val )

            if isinstance(value_var, str):
                value_var = float(value_var)

            value_help_text = var_data.get('help_text') or ""

            help_text_split = value_help_text.split('|')
            str_unidad_medida = help_text_split[1].strip() if len(help_text_split) > 1 else ""

            # str_unidad_medida = str_unidad_medida.split('|')[0].strip()
            
            if min_var and not max_var:
                str_rango = f"> {min_var} {str_unidad_medida}"
                # Se evalúa solo el mínimo
                if value_var < min_var:
                    list_items_to_group.append( self.prepare_data_to_group(var_data, value_var, str_rango, value_as_str) )
            elif max_var and not min_var:
                str_rango = f"< {max_var} {str_unidad_medida}"
                # Se evalúa solo el máximo
                if value_var > max_var:
                    list_items_to_group.append( self.prepare_data_to_group(var_data, value_var, str_rango, value_as_str) )
            elif not min_var <= value_var <= max_var:
                str_rango = f"{min_var} - {max_var} {str_unidad_medida}"
                # Si llega hasta acá, hay minimo y maximo definido, se evalua que el valor esté dentro del rango
                list_items_to_group.append( self.prepare_data_to_group(var_data, value_var, str_rango, value_as_str) )

        return list_items_to_group

    
    def bitacora_de_clarificacion(self):
        # Se obtienen los campos que se ocupan como variables, capturados por el usuario y desde readonly
        fields_to_group, fields_to_catalog, fields_generals, value_as_str = self.get_fields_variables()
        # print('\n === fields_to_group =',simplejson.dumps(fields_to_group, indent=4))
        # print('\n === fields_to_catalog =',simplejson.dumps(fields_to_catalog, indent=4))
        # print('\n === fields_generals =',simplejson.dumps(fields_generals, indent=4))

        # Se obtienen los máximos y mínimos desde el catálogo de configuraciones
        config_ranges_variables = self.get_configs_catalog_variables()
        if not config_ranges_variables:
            print('No se encontraron los registros de configuracion')
            return False

        group_vars_fuera_de_rango = []

        # Para la forma Bitácora de Energía (134982) se requiere realizar un cálculo para el Total de Niveles de Tanque
        is_form_bitacora_energia = self.form_id == self.FORM_BITACORA_ENERGIA
        if is_form_bitacora_energia:
            total_niveles_tanque = sum([ 
                answers.get( self.f[ field_tanque ], 0 ) 
                for field_tanque in [
                    'nivel_tanque_1', 'nivel_tanque_2', 'nivel_tanque_3', 'nivel_tanque_4', 'nivel_tanque_5', 'nivel_tanque_6', 'nivel_tanque_7', 'nivel_tanque_8', 'nivel_tanque_9'
                ]
            ])

            print('total_niveles_tanque =',total_niveles_tanque)

            answers[ self.f['total_niveles_tanque'] ] = total_niveles_tanque
        
        # Se procesan las variables que están dentro de grupo repetitivo para armar la estructura que necesito
        for field_group, fields_variables in fields_to_group.items():
            for item_group in answers.get( field_group, [] ):
                group_vars_fuera_de_rango.extend( self.process_variables(fields_variables, config_ranges_variables, item_group, value_as_str) )

        # Se procesan las variables que están en un catálogo
        for field_catalog, fields_variables in fields_to_catalog.items():
            group_vars_fuera_de_rango.extend( self.process_variables(fields_variables, config_ranges_variables, answers.get(field_catalog, {}), value_as_str, processing_catalog_values=True) )

        # Se procesan las variables que están a primer nivel del answers
        group_vars_fuera_de_rango.extend( self.process_variables(fields_generals, config_ranges_variables, answers, value_as_str) )

        # Se integra al answers el grupo de Variables fuera de rango
        # print('=== group vars = ', simplejson.dumps(group_vars_fuera_de_rango, indent=4))

        if group_vars_fuera_de_rango or is_form_bitacora_energia:
            if group_vars_fuera_de_rango:
                answers[ self.f['group_fuera_de_rango'] ] = group_vars_fuera_de_rango

            # Se agrega el grupo repetitivo al answers del registro que se esta enviando
            sys.stdout.write(simplejson.dumps({
                'status': 101,
                'replace_ans': answers
            }))

    def get_configs_catalog_variables(self):
        """
        Consulta el catalogo Configuracion de variables de produccion
        """
        record_config = self.lkf_api.search_catalog(self.CATALOG_CONFIGS)
        if not record_config:
            return {}

        return record_config[0]

    def get_fields_variables(self):
        """
        Se consulta la estructura de la forma y se obtienen los campos que seran necesarios para 
        el grupo de variables
        """
        form_fields = lkf_obj.lkf_api.get_form_id_fields(lkf_obj.form_id)
        fields = form_fields[0]['fields']

        # Se procesan los campos y se obtienen los que empiecen por aaaaa
        fields_to_group, fields_to_catalog, fields_generals = {}, {}, {}
        value_as_str = False

        for f in fields:
            field_id = f.get('field_id', '')

            if field_id == self.f['fuera_rango_valor_obtenido'] and f['field_type'] == 'text':
                value_as_str = True

            if re.match(r"^(aaaaa|ccaaa)", field_id):

                data_field = {
                    'label': f['label'], 
                    'field_type': f['field_type'],
                    'help_text': f['help_text']
                }

                catalog_field_id = f['catalog'].get('catalog_field_id') if f.get('catalog') else None
                group_id = f['group'].get('group_id') if f.get('group') else None

                if group_id:
                    fields_to_group.setdefault(group_id, {})[field_id] = data_field
                elif catalog_field_id:
                    fields_to_catalog.setdefault(catalog_field_id, {})[field_id] = data_field
                else:
                    fields_generals[field_id] = data_field
        
        # fields_variables = {
        #     f['field_id']: {
        #         'label': f['label'], 
        #         'field_type': f['field_type'],
        #         'help_text': f['help_text'],
        #         'catalog_field_id': f['catalog'].get('catalog_field_id') if f.get('catalog') else None,
        #         'group_id': f['group'].get('group_id') if f.get('group') else None
        #     }
        #     for f in fields
        #     if re.match(r"^(aaaaa|ccaaa)", f.get('field_id', ''))
        # }
        return fields_to_group, fields_to_catalog, fields_generals, value_as_str


if __name__ == '__main__':
    lkf_obj = Custom(settings, sys_argv=sys.argv)
    # lkf_obj.console_run()

    answers = lkf_obj.answers

    # Metiendo datos para mis pruebas
    # answers['6834941785aed7781f27cb40'] = [{
    #     '67363f34ad09ce75b98e7e63': {'67363f34ad09ce75b98e7e64': 'nombre clarificador'},
    #     '6834952227088d7137d46a8e': 'en_linea',
    #     'aaaaa5ddd4756a262cbd080f': 500,
    #     '683495ddd4756a262cbd0812': 50
    # },{
    #     '67363f34ad09ce75b98e7e63': {'67363f34ad09ce75b98e7e64': 'nombre clarificador'},
    #     '6834952227088d7137d46a8e': 'no_linea',
    #     'aaaaa5ddd4756a262cbd080f': 700,
    #     '683495ddd4756a262cbd0812': 70
    # }]

    # answers['68375178ec91ea1bc3e92b64'] = {
    #     '683753204328adb3fa0bfd2b': '2025-06-07',
    #     'ccaaa5b15ad84734fae92bab': [351],
    #     'ccaaa5b15ad84734fae92bac': [7]
    # }

    # answers['aaaaa9ae6252bddeaede7911'] = 50
    # answers['aaaaaff28802aa99a480d35e'] = 1.2

    # Pruebas de valores con Fracciones
    # answers['aaaaa0c7d75b0e3f4ba2c326'] = "25 / 2"

    lkf_obj.bitacora_de_clarificacion()