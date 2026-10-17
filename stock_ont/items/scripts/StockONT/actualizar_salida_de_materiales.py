# -*- coding: utf-8 -*-
import sys
from copy import deepcopy
from salida_de_materiales import Stock
from account_settings import *

class Stock(Stock):
    """docstring for Stock"""
    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        self.inv_map_salida_phases = {v: k for k, v in self.map_salida_phases.items()}

        # Permiso de la tabla de SIGA (seccion Salidas) que se necesita para
        # pasar la Salida a cada estatus. 'delivered' depende de si la entrega
        # es local o foranea, ver get_salida_action().
        # TODO: revisar con operaciones. 'pending_authorization' lo asigna
        # generar_salidas_por_contratista.py al crear la Salida ("Generar
        # calculo de reabastecimiento"); aqui solo aplica si se vuelve a
        # editar antes de autorizarla.
        self.map_salida_stage_action = {
            'pending_authorization': 'calculation',
            'voucher_generated': 'authorize_voucher',
            'in_preparation': 'prepare_material',
            'ready_to_deliver': 'prepare_material',
            # TODO: revisar con operaciones. La bifurcacion local/foranea al
            # salir de Listo para Entrega no tiene permiso propio en la tabla;
            # se asume que la hace quien prepara el material.
            'awaiting_local_delivery': 'prepare_material',
            'awaiting_transit': 'capture_dispatch',
            'awaiting_supervisor_review': 'confirm_transit',
            # TODO: revisar con operaciones. No esta claro si la "Revision de
            # supervisor" va aqui (valida la llegada, En transito -> Por
            # entregar) o antes de despachar (Listo para Entrega -> Alistando
            # despacho).
            'awaiting_final_delivery': 'supervisor_review',
        }

        # super_admin y national_admin pueden pasar a cualquier estatus
        self.salida_full_access_roles = ('super_admin', 'national_admin')
        roles_op = ('wh_operator_puebla', 'wh_operator_camarones')
        # TODO: revisar con operaciones. warehouse_manager no tiene ningun
        # permiso operativo en Salidas; seria el candidato natural para
        # 'supervisor_review' y quiza para 'authorize_voucher' junto con CO.
        self.map_salida_action_roles = {
            'calculation': ('wh_admin_assistant_puebla', 'wh_admin_assistant_camarones'),
            'authorize_voucher': ('warehouse_coordinator',),
            # TODO: revisar con operaciones. 'prepare_material' cubre dos
            # transiciones (iniciar y terminar la preparacion); separarlo si
            # se quiere distinguir quien empieza y quien termina.
            'prepare_material': roles_op,
            'capture_dispatch': roles_op,
            'confirm_transit': roles_op,
            'confirm_local_delivery': roles_op,
            'supervisor_review': (),
            # TODO: revisar con operaciones. La tabla lo marca como almacen
            # Origen y solo para SA/NA, pero lo logico es que confirme quien
            # recibe (almacen Destino); field_receiver no tiene ningun permiso
            # en Salidas y parece ser quien deberia tenerlo.
            'confirm_final_delivery': (),
        }

        # Almacen de cada rol de WH; los roles sin almacen no se restringen por almacen
        self.map_salida_role_location = {
            'wh_operator_puebla': 'PCI Puebla',
            'wh_operator_camarones': 'Camarones',
            'wh_admin_assistant_puebla': 'PCI Puebla',
            'wh_admin_assistant_camarones': 'Camarones',
        }

    def get_salida_role(self):
        """
        Rol de SIGA del usuario: primero los de administracion (ver
        is_user_admin) y despues los grupos de WH.

        Returns:
            str: rol, 'no_user_found' sin jwt valido, o '' si no esta en ningun grupo.
        """
        is_admin, type_admin = self.is_user_admin()
        if is_admin or type_admin == 'no_user_found':
            return type_admin

        for role, id_group in self.siga_wh_operator_groups + self.siga_wh_manager_groups + self.siga_wh_admin_assistant_groups:
            if self.current_user_id in self._get_users_group(id_group):
                return role

        return ''

    def get_salida_action(self, stage, answers_record):
        """
        Permiso que se necesita para pasar la Salida a `stage`. Para
        'delivered' se distingue la entrega local de la foranea con
        `delivery.isForanea` del payload o, si no viene, con el
        delivery_type guardado.
        """
        if stage != 'delivered':
            return self.map_salida_stage_action.get(stage)

        is_foranea = (self.data.get('delivery') or {}).get('isForanea')
        if is_foranea is None:
            is_foranea = self.unlist(answers_record.get(self.f['delivery_type'])) == self.format_delivery_type(True)

        return 'confirm_final_delivery' if is_foranea else 'confirm_local_delivery'

    def validate_salida_permission(self, answers_record):
        """
        Valida que el rol del usuario pueda pasar la Salida al estatus enviado
        (`self.data['stage']`) y que sea del almacen de origen. Se valida el
        almacen guardado en la BD y el del payload, porque
        build_answers_salida() reemplaza el almacen origen.

        Args:
            answers_record (dict): answers del registro guardado.

        Returns:
            dict | None: respuesta 403 si no tiene permiso, None si pasa.
        """
        role = self.get_salida_role()
        if role in self.salida_full_access_roles:
            return None

        stage = self.data.get('stage')
        action = self.get_salida_action(stage, answers_record)
        if not action or role not in self.map_salida_action_roles[action]:
            return {
                'status_code': 403,
                'msg': f"El rol '{role}' no puede pasar Salidas de Material a '{stage}'"
            }

        # TODO: revisar con operaciones. Todos los permisos de Salidas se
        # validan contra el almacen Origen (ver TODO de 'confirm_final_delivery').
        almacen_guardado = answers_record.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID) or {}
        locations = [
            self.unlist(almacen_guardado.get(self.stk.WH.f['warehouse_location'])),
            self.data.get('originWarehouse'),
        ]

        location_role = self.map_salida_role_location.get(role)
        for location in locations:
            if location_role and location != location_role:
                return {
                    'status_code': 403,
                    'msg': f"Usuario no autorizado para operar Salidas de Material del almacen '{location}'"
                }

        return None

    def patch_salida_materiales(self, record_id, answers):
        """
        Actualiza el registro existente en la forma de Salida de Material
        (form_id FORM_ID_SALIDAS) con las respuestas ya armadas.

        Args:
            record_id: _id del registro a actualizar.
            answers (dict): respuestas {field_id: valor} a guardar.

        Returns:
            dict: respuesta de `lkf_api.patch_record`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_SALIDAS, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Salida de Material",
                    "action": "Actualizar Salida de Material",
                    "script": "actualizar_salida_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_salida_materiales",
                }
            },
            'answers': answers,
        })
        return self.lkf_api.patch_record(metadata, record_id=record_id)

    def build_grp_stages_merge(self, existing_rows):
        """
        A diferencia de build_grp_stages() (que reconstruye calculation/
        authorization/preparation desde cero a partir de self.data), aqui se
        conservan las fases ya guardadas en la forma (existing_rows, tal cual
        se leyeron de la BD) y solo se sobreescribe o agrega la(s) fase(s)
        que vengan en self.data en esta edicion.

        Args:
            existing_rows (list[dict]): filas de field_grp_stages ya guardadas.

        Returns:
            list[dict]: filas para el campo field_grp_stages.
        """
        phases_by_key = {}
        for row_stage in existing_rows:
            phase_key = self.inv_map_salida_phases.get(self.unlist(row_stage.get(self.f['field_stage_name'])))
            if phase_key:
                phases_by_key[phase_key] = row_stage

        for stage, name_stage in self.map_salida_phases.items():
            data_stage = self.data.get(stage)
            if not data_stage:
                continue
            row_stage = self._build_stage_row(stage, name_stage, data_stage)
            if row_stage:
                phases_by_key[stage] = row_stage

        return [phases_by_key[stage] for stage in self.map_salida_phases if stage in phases_by_key]

    def keep_saved_fields(self, answers_salida, original_answers):
        """
        patch_record reemplaza todos los answers, y el front no manda los
        datos del calculo (periodo/areas/tecnologias/No. Instalaciones) ni el
        folio del Vale de Materiales generado; se toman del registro guardado
        para que no se pierdan en el patch.

        Args:
            answers_salida (dict): respuestas ya armadas de la Salida (se modifica).
            original_answers (dict): answers del registro antes del patch.
        """
        for field_key in ('field_vale_periodo', 'field_vale_area', 'field_vale_tecnologia',
                          'field_vale_num_instalaciones', 'field_salida_folio_vale'):
            field_id = self.f[field_key]
            if field_id not in answers_salida and field_id in original_answers:
                answers_salida[field_id] = original_answers[field_id]

    def build_answers_stock_one_many_one(self, answers_salida):
        """
        Arma las respuestas para el nuevo registro de salida en
        STOCK_ONE_MANY_ONE. El almacen origen y el destino (el del
        finalContratista, ver build_answers_recipient) se toman de las
        respuestas ya armadas de la Salida, para no volver a consultar los
        catalogos; los items salen de self.data.

        Args:
            answers_salida (dict): respuestas ya armadas de la Salida.

        Returns:
            dict: respuestas {field_id: valor} para STOCK_ONE_MANY_ONE.
        """
        return {
            self.f['fecha_recepcion']: self.today_str(date_format='datetime'),
            self.f['stock_status']: 'to_do',
            self.f['stock_move_comments']: f"Salida entregada - Salida de Material {self.data.get('folio')}",
            self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID: answers_salida.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID),
            self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID: answers_salida.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID),
            self.f['move_group']: self.build_move_group_lines(),
        }

    def generar_traspaso_stock_one_many_one(self, answers_salida):
        answers_stock_one_many_one = self.build_answers_stock_one_many_one(answers_salida)
        return self.post_stock_one_many_one(answers_stock_one_many_one, {
            "process": "Salida de Material",
            "action": "Generar Salida por Entrega de Material",
            "script": "actualizar_salida_de_materiales.py",
            "function": "actualizar_salida_materiales",
        })

    def build_grp_materiales_vale(self):
        """
        Arma el grupo repetitivo de materiales del Vale de Materiales: una
        fila por sku con la cantidad entregada (misma regla que el traspaso,
        ver get_delivered_quantity). Se omiten los items sin cantidad entregada.

        Returns:
            list[dict]: filas para el campo self.f['field_vale_grp_materiales'].
        """
        materiales_by_sku = {}
        for item in self.data.get('items', []):
            sku = item.get('sku')
            cantidad = self.get_delivered_quantity(item)
            if not cantidad:
                continue

            if sku not in materiales_by_sku:
                info_catalog_sku = self.find_material_catalog_sku(sku, field_as_select=[self.f['field_product_code']])
                if not info_catalog_sku:
                    print(f"ADVERTENCIA: no se encontro el sku '{sku}' en el catalogo")
                    continue

                materiales_by_sku[sku] = {
                    self.f['obj_vale_sku']: {
                        self.f['field_product_code']: info_catalog_sku.get(self.f['field_product_code']),
                        self.f['field_sku']: info_catalog_sku.get(self.f['field_sku']),
                        self.f['field_product_name']: info_catalog_sku.get(self.f['field_product_name']),
                        self.f['tipo_material']: info_catalog_sku.get(self.f['tipo_material']),
                        self.f['field_unidad_medida']: info_catalog_sku.get(self.f['field_unidad_medida']),
                    },
                    self.f['field_vale_cantidad']: 0,
                }

            materiales_by_sku[sku][self.f['field_vale_cantidad']] += cantidad

        return list(materiales_by_sku.values())

    def build_answers_vale_materiales(self, answers_salida):
        """
        Arma las respuestas del Vale de Materiales a partir de las respuestas
        ya armadas de la Salida: contratista y almacenes (se copia el contenido
        con los ids de grupo del Vale, los campos internos son los mismos) y
        los datos del calculo (periodo/areas/tecnologias/No. Instalaciones,
        mismo id en ambas formas, ver keep_saved_fields).

        Args:
            answers_salida (dict): respuestas ya armadas de la Salida.

        Returns:
            dict: respuestas {field_id: valor} para FORM_ID_VALES_MATERIALES.
        """
        answers_vale = {
            self.f['field_vale_fecha_corte']: self.today_str(),
            self.f['obj_vale_contratista']: answers_salida.get(self.f['obj_catalog_contratistas']),
            self.f['obj_vale_wh_origen']: answers_salida.get(self.stk.WH.WAREHOUSE_LOCATION_OBJ_ID),
            self.f['obj_vale_wh_destino']: answers_salida.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID),
            self.f['field_vale_grp_materiales']: self.build_grp_materiales_vale(),
        }
        for field_key in ('field_vale_periodo', 'field_vale_area', 'field_vale_tecnologia', 'field_vale_num_instalaciones'):
            answers_vale[self.f[field_key]] = answers_salida.get(self.f[field_key])

        return answers_vale

    def get_next_folio_vale(self):
        """
        Siguiente folio del Vale de Materiales: ultimo folio numerico + 1 a 8
        digitos (mismo criterio que get_last_record_vale() en
        infosync_scripts/PCI/crear_vales_de_materiales.py).
        """
        last_record_vale = self.cr.find_one(
            {'form_id': self.FORM_ID_VALES_MATERIALES, 'deleted_at': {'$exists': False}},
            {'folio': 1},
            sort=[('created_at', -1)]
        )
        try:
            last_folio = int((last_record_vale or {}).get('folio', 0))
        except (TypeError, ValueError):
            last_folio = 0
        return str(last_folio + 1).zfill(8)

    def crear_vale_materiales(self, answers_salida):
        """
        Crea el registro de Vale de Materiales (FORM_ID_VALES_MATERIALES) por
        la entrega de la Salida.

        Returns:
            dict: respuesta de `lkf_api.post_forms_answers`.
        """
        metadata = self.lkf_api.get_metadata(self.FORM_ID_VALES_MATERIALES, user_id=self.record_user_id)
        metadata.update({
            'properties': {
                "device_properties": {
                    "system": "Script",
                    "process": "Salida de Material",
                    "action": "Crear Vale de Materiales por Entrega de Material",
                    "script": "actualizar_salida_de_materiales.py",
                    "module": "stock_ont",
                    "function": "actualizar_salida_materiales",
                    "folio_salida": self.data.get('folio'),
                }
            },
            'answers': self.build_answers_vale_materiales(answers_salida),
        })
        metadata['folio'] = self.get_next_folio_vale()
        return self.lkf_api.post_forms_answers(metadata, jwt_settings_key='JWT_ADMIN')

    def set_folio_vale_en_salida(self, folio_query, folio_vale):
        """
        Guarda el folio del Vale generado directo en Mongo (mismo query que
        get_record_by_folio), para no hacer un segundo patch_record de la Salida.
        """
        return self.cr.update_one(
            {'folio': folio_query, 'form_id': self.FORM_ID_SALIDAS, 'deleted_at': {'$exists': False}},
            {'$set': {f"answers.{self.f['field_salida_folio_vale']}": folio_vale}}
        )

    def actualizar_salida_materiales(self):
        """
        Recibe en self.data el `id` (folio) del registro a editar (mas el
        resto del payload con la misma estructura que salida_de_materiales()),
        y actualiza la forma via patch_record. `items`, `delivery`, `events`,
        `stage` y el almacen origen se reemplazan por completo con lo que
        venga en self.data; las fases (calculation/authorization/preparation)
        se conservan y solo se agrega/actualiza la(s) que vengan en este
        payload (ver build_grp_stages_merge).
        """
        folio = self.data.get('folio')
        if not folio:
            self.LKFException("No se recibio el id/folio de la Salida de Material a actualizar")

        folio_query = int(folio) if str(folio).isdigit() else folio
        record = self.get_record_by_folio(folio_query, self.FORM_ID_SALIDAS)
        if not record:
            self.LKFException(f"No se encontro ninguna Salida de Material con folio '{folio}'")

        # Copia de los answers antes del patch, para restaurarlos si falla el traspaso.
        original_answers = deepcopy(record.get('answers', {}))

        resp_permiso = self.validate_salida_permission(original_answers)
        if resp_permiso:
            return resp_permiso
        existing_stage_rows = original_answers.get(self.f['field_grp_stages']) or []
        existing_status = self.unlist(original_answers.get(self.f['field_status_transferencia']))

        answers_salida = self.build_answers_salida()
        answers_salida[ self.f['field_grp_stages'] ] = self.build_grp_stages_merge(existing_stage_rows)
        self.keep_saved_fields(answers_salida, original_answers)

        # Solo se genera el traspaso cuando la Salida pasa a entregado, para no
        # duplicar el movimiento de stock si se vuelve a editar ya entregada.
        status_entregado = self.map_salida_stages['delivered']
        generar_traspaso = answers_salida[ self.f['field_status_transferencia'] ] == status_entregado \
            and existing_status != status_entregado

        if generar_traspaso and not answers_salida.get(self.stk.WH.WAREHOUSE_LOCATION_DEST_OBJ_ID):
            self.LKFException(
                f"No se encontro el almacen destino del contratista '{(self.data.get('recipient') or {}).get('finalContratista')}', "
                "no se puede generar el traspaso de la Salida de Material"
            )

        resp_actualizar = self.patch_salida_materiales(record['_id'], answers_salida)

        if generar_traspaso:
            resp_stock = self.generar_traspaso_stock_one_many_one(answers_salida)
            resp_actualizar['stock_one_many_one'] = resp_stock
            if resp_stock.get('status_code') != 201:
                self.patch_salida_materiales(record['_id'], original_answers)
                resp_actualizar['status_code'] = 400
                resp_actualizar['error'] = "Ocurrio un error al generar la salida del Stock"
                return resp_actualizar

            # El folio guardado evita duplicar el Vale si se reprocesa la entrega.
            if not answers_salida.get(self.f['field_salida_folio_vale']):
                resp_vale = self.crear_vale_materiales(answers_salida)
                resp_actualizar['vale_materiales'] = resp_vale
                if resp_vale.get('status_code') == 201:
                    folio_vale = (resp_vale.get('json') or {}).get('folio')
                    self.set_folio_vale_en_salida(folio_query, folio_vale)
                else:
                    # El traspaso de stock ya se genero y no se revierte: solo se reporta el error.
                    resp_actualizar['error_vale_materiales'] = "Se genero la salida del Stock pero ocurrio un error al crear el Vale de Materiales"

        if self.data.get('requestExternalSignature'):
            resp_firma = self.create_record_firma_externa(record.get('folio'), 'salida', 'actualizar_salida_de_materiales.py')
            if resp_firma:
                resp_actualizar['firma_externa'] = resp_firma

        return resp_actualizar

if __name__ == '__main__':
    stock_obj = Stock(settings, sys_argv=sys.argv)
    stock_obj.console_run()
    resp_actualizar_salida = stock_obj.actualizar_salida_materiales()
    stock_obj.HttpResponse({"data": resp_actualizar_salida})
