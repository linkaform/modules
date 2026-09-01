# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from menus import Base

class Base(Base):

    PLATFORM_LABELS = {'web': 'Web', 'mobile': 'Mobile'}

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)

        self.usuarios_fields = {
            "user_id": "638a9a99616398d2e392a9f5",
            "nombre": "638a9a7767c332f5d459fc81",
            "username": "6759e4a7a9a6e13c7b26da33",
            "email": "638a9a7767c332f5d459fc82",
        }

    def _catalog_item_to_dict(self, item):
        f = self.menu_catalog_fields
        platforms_raw = item.get(f['catalog_plataforms']) or ''
        return {
            "_id": item.get('_id'),
            "menu_key": item.get(f['catalog_menu_key']),
            "menu": item.get(f['catalog_menu']),
            "menu_order": item.get(f['catalog_menu_order']),
            "menu_icon": item.get(f['catalog_menu_icon']),
            "menu_columns": item.get(f['catalog_menu_columns']),
            "seccion_key": item.get(f['catalog_seccion_key']),
            "seccion": item.get(f['catalog_seccion']),
            "seccion_order": item.get(f['catalog_seccion_order']),
            "seccion_column": item.get(f['catalog_seccion_column']),
            "seccion_href": item.get(f['catalog_seccion_href']),
            "seccion_icon": item.get(f['catalog_seccion_icon']),
            "seccion_icon_color": item.get(f['catalog_seccion_icon_color']),
            "elemento": item.get(f['catalog_elemento']),
            "key": item.get(f['catalog_key']),
            "type": item.get(f['catalog_type']),
            "item_order": item.get(f['catalog_item_order']),
            "href_web": item.get(f['catalog_href_web']),
            "route_mobile": item.get(f['catalog_route_mobile']),
            "platforms": platforms_raw.lower(),
            "item_icon": item.get(f['catalog_item_icon']),
            "seccion_description": item.get(f['catalog_seccion_description']),
        }

    def list_menu_items(self):
        """
        Regresa todos los items del catálogo ELEMENTOS MENU (todos los módulos),
        para la tabla de administración de menús.
        """
        mango_query = {"selector": {}, "limit": 10000}
        data = self.lkf_api.search_catalog(self.MENUS_CATALOG_ID, mango_query)
        items = [self._catalog_item_to_dict(item) for item in data]
        items.sort(key=lambda i: (
            i.get('menu_order') or 0,
            i.get('seccion_order') or 0,
            i.get('item_order') or 0,
        ))
        return items

    def _find_catalog_record(self, record_id):
        mango_query = {"selector": {}, "limit": 10000}
        data = self.lkf_api.search_catalog(self.MENUS_CATALOG_ID, mango_query)
        for item in data:
            if item.get('_id') == record_id:
                return item
        return None

    def _save_one_item(self, payload):
        f = self.menu_catalog_fields
        platform_value = (payload.get('platforms') or 'web').lower()
        answers = {
            f['catalog_menu_key']: payload.get('menu_key'),
            f['catalog_menu']: payload.get('menu'),
            f['catalog_menu_order']: int(payload.get('menu_order') or 0),
            f['catalog_menu_icon']: payload.get('menu_icon') or '',
            f['catalog_menu_columns']: int(payload.get('menu_columns') or 0),
            f['catalog_seccion_key']: payload.get('seccion_key'),
            f['catalog_seccion']: payload.get('seccion'),
            f['catalog_seccion_order']: int(payload.get('seccion_order') or 0),
            f['catalog_seccion_column']: int(payload.get('seccion_column') or 0),
            f['catalog_seccion_href']: payload.get('seccion_href') or '',
            f['catalog_seccion_icon']: payload.get('seccion_icon') or '',
            f['catalog_seccion_icon_color']: payload.get('seccion_icon_color') or '',
            f['catalog_elemento']: payload.get('elemento'),
            f['catalog_key']: payload.get('key'),
            f['catalog_type']: payload.get('type') or 'link',
            f['catalog_item_order']: int(payload.get('item_order') or 0),
            f['catalog_href_web']: payload.get('href_web') or '',
            f['catalog_route_mobile']: payload.get('route_mobile') or '',
            # LinkaForm valida este radio contra el LABEL ("Web"), no el value ("web")
            f['catalog_plataforms']: self.PLATFORM_LABELS.get(platform_value, 'Web'),
            f['catalog_item_icon']: payload.get('item_icon') or '',
            f['catalog_seccion_description']: payload.get('seccion_description') or '',
        }
        metadata = self.lkf_api.get_catalog_metadata(catalog_id=self.MENUS_CATALOG_ID)
        metadata['answers'] = answers
        record_id = payload.get('_id')
        if record_id:
            res = self.lkf_api.update_catalog_answers(metadata, record_id=record_id)
        else:
            res = self.lkf_api.post_catalog_answers(metadata)
        return res

    def save_menu_item(self, payload):
        """
        Crea o actualiza (si trae _id) un item del catálogo ELEMENTOS MENU.
        """
        results = self.save_menu_items_batch([payload])
        return results[0] if results else {}

    def save_menu_items_batch(self, items):
        """
        Crea o actualiza varios items del catálogo ELEMENTOS MENU en un solo llamado
        (usado por el tablero drag-and-drop, que recalcula ordenes/columnas de
        todo un módulo de una vez). Si el label de sección o elemento de un item
        existente cambió, sincroniza esa copia en todos los usuarios que ya
        tengan esa key asignada (CONFIGURACION_MENUS).
        """
        f = self.menu_catalog_fields
        existing_by_id = {
            item.get('_id'): item
            for item in self.lkf_api.search_catalog(self.MENUS_CATALOG_ID, {"selector": {}, "limit": 10000})
        }

        results = []
        changed_keys = []
        for payload in items or []:
            record_id = payload.get('_id')
            old = existing_by_id.get(record_id) if record_id else None
            results.append(self._save_one_item(payload))
            if old and payload.get('key') and (
                payload.get('seccion') != old.get(f['catalog_seccion'])
                or payload.get('elemento') != old.get(f['catalog_elemento'])
            ):
                changed_keys.append(payload.get('key'))

        if changed_keys:
            self._sync_item_labels_to_users(changed_keys, items)

        return results

    def _sync_item_labels_to_users(self, changed_keys, items):
        """
        Actualiza la copia de menu/seccion/elemento/plataforms guardada en
        CONFIGURACION_MENUS para los usuarios que ya tienen asignada alguna
        de las keys cuyo label de sección o elemento cambió.
        """
        changed_set = {k for k in changed_keys if k}
        if not changed_set:
            return {"updated_records": 0}

        new_data_by_key = {i.get('key'): i for i in items if i.get('key') in changed_set}
        key_field = self.menu_form_fields['key']

        query = [
            {"$match": {"form_id": self.MENUS_FORM, "deleted_at": {"$exists": False}}},
            {"$project": {
                "_id": 1,
                "elementos": f"$answers.{self.menu_form_fields['elementos']}",
                "usuario_obj": f"$answers.{self.USUARIOS_OBJ_ID}",
            }}
        ]
        records = self.format_cr(self.cr.aggregate(query), labels_off=True)

        updated = 0
        for record in records:
            elementos = record.get('elementos') or []
            changed = False
            new_elementos = []
            for elemento in elementos:
                obj = elemento.get(self.MENUS_CATALOG_OBJ_ID, {})
                raw_key = obj.get(key_field)
                item_key = self.unlist(raw_key) if raw_key else None
                new_item = new_data_by_key.get(item_key)
                if new_item:
                    new_obj = dict(obj)
                    new_obj[self.menu_form_fields['menu']] = new_item.get('menu')
                    new_obj[self.menu_form_fields['seccion']] = new_item.get('seccion')
                    new_obj[self.menu_form_fields['elemento']] = new_item.get('elemento')
                    new_obj[self.menu_form_fields['plataforms']] = [new_item.get('platforms')]
                    new_elementos.append({self.MENUS_CATALOG_OBJ_ID: new_obj})
                    changed = True
                else:
                    new_elementos.append(elemento)

            if not changed:
                continue

            metadata = self.lkf_api.get_metadata(form_id=self.MENUS_FORM)
            metadata['_id'] = record['_id']
            metadata['answers'] = {
                self.USUARIOS_OBJ_ID: record.get('usuario_obj') or {},
                self.menu_form_fields['elementos']: new_elementos,
            }
            self.net.patch_forms_answers(metadata)
            updated += 1

        return {"updated_records": updated}

    def replace_menu_catalog(self, items):
        """
        Reemplaza TODO el catálogo ELEMENTOS MENU: borra los registros
        existentes y crea uno nuevo por cada item recibido (importación de
        Excel). Las keys que existían antes y ya no vienen en el import se
        limpian también de cualquier usuario que las tuviera asignadas.
        """
        f = self.menu_catalog_fields
        existing = self.lkf_api.search_catalog(self.MENUS_CATALOG_ID, {"selector": {}, "limit": 10000})
        existing_keys = {row.get(f['catalog_key']) for row in existing}
        new_keys = {i.get('key') for i in (items or [])}
        removed_keys = [k for k in existing_keys if k and k not in new_keys]

        for row in existing:
            self.lkf_api.delete_catalog_record(self.MENUS_CATALOG_ID, row.get('_id'), row.get('_rev'))

        created = 0
        for payload in items or []:
            create_payload = dict(payload)
            create_payload.pop('_id', None)
            self._save_one_item(create_payload)
            created += 1

        if removed_keys:
            self._cleanup_deleted_item_keys(removed_keys)

        return {
            "deleted": len(existing),
            "created": created,
            "cleaned_keys": len(removed_keys),
        }

    def delete_menu_item(self, record_id):
        """
        Borra un item del catálogo ELEMENTOS MENU y limpia esa key de
        cualquier usuario que la tuviera asignada (CONFIGURACION_MENUS) —
        solo si ninguna otra fila del catálogo (ej. la misma key en la otra
        plataforma) sigue usando esa key.
        """
        record = self._find_catalog_record(record_id)
        if not record:
            return {"error": "Registro no encontrado", "status_code": 404}
        key = record.get(self.menu_catalog_fields['catalog_key'])
        res = self.lkf_api.delete_catalog_record(self.MENUS_CATALOG_ID, record_id, record.get('_rev'))
        if key and not self._key_still_in_catalog(key, exclude_record_id=record_id):
            self._cleanup_deleted_item_keys([key])
        return res

    def _key_still_in_catalog(self, key, exclude_record_id=None):
        key_field = self.menu_catalog_fields['catalog_key']
        rows = self.lkf_api.search_catalog(self.MENUS_CATALOG_ID, {"selector": {}, "limit": 10000})
        return any(
            row.get(key_field) == key
            for row in rows
            if row.get('_id') != exclude_record_id
        )

    def _cleanup_deleted_item_keys(self, deleted_keys):
        """
        Quita las keys borradas del catálogo de la lista `elementos` de
        cualquier registro CONFIGURACION_MENUS que las tuviera asignadas.
        """
        deleted_set = set(deleted_keys)
        key_field = self.menu_form_fields['key']
        query = [
            {"$match": {
                "form_id": self.MENUS_FORM,
                "deleted_at": {"$exists": False},
            }},
            {"$project": {
                "_id": 1,
                "elementos": f"$answers.{self.menu_form_fields['elementos']}",
                "usuario_obj": f"$answers.{self.USUARIOS_OBJ_ID}",
            }}
        ]
        records = self.format_cr(self.cr.aggregate(query), labels_off=True)
        updated = 0
        for record in records:
            elementos = record.get('elementos') or []

            def keeps(elemento):
                raw_key = elemento.get(self.MENUS_CATALOG_OBJ_ID, {}).get(key_field)
                item_key = self.unlist(raw_key) if raw_key else None
                return item_key not in deleted_set

            kept = [e for e in elementos if keeps(e)]
            if len(kept) == len(elementos):
                continue

            metadata = self.lkf_api.get_metadata(form_id=self.MENUS_FORM)
            metadata['_id'] = record['_id']
            metadata['answers'] = {
                self.USUARIOS_OBJ_ID: record.get('usuario_obj') or {},
                self.menu_form_fields['elementos']: kept,
            }
            self.net.patch_forms_answers(metadata)
            updated += 1
        return {"updated_records": updated}

    def list_users(self):
        """
        Regresa los usuarios de la cuenta (catálogo USUARIOS) para el picker
        de asignación de menús.
        """
        f = self.usuarios_fields
        mango_query = {"selector": {}, "limit": 10000}
        data = self.lkf_api.search_catalog(self.Accesos.USUARIOS_ID, mango_query)
        users_by_id = {}
        for item in data:
            user_id = item.get(f['user_id'])
            if not user_id or user_id in users_by_id:
                continue
            users_by_id[user_id] = {
                "user_id": user_id,
                "nombre": item.get(f['nombre']) or '',
                "username": item.get(f['username']) or '',
                "email": item.get(f['email']) or '',
            }
        users = list(users_by_id.values())
        users.sort(key=lambda u: (u.get('nombre') or u.get('username') or '').lower())
        return users

    def _get_user_menu_record(self, user_id):
        query = [
            {"$match": {
                "form_id": self.MENUS_FORM,
                "deleted_at": {"$exists": False},
                f"answers.{self.USUARIOS_OBJ_ID}.{self.menu_form_fields['usuario_id']}": user_id
            }},
            {"$project": {
                "_id": 1,
                "elementos": f"$answers.{self.menu_form_fields['elementos']}"
            }}
        ]
        data = self.format_cr(self.cr.aggregate(query), get_one=True, labels_off=True)
        return data

    def get_user_menu_items(self, user_id):
        """
        Regresa las keys de items de menú actualmente asignadas a un usuario.
        """
        record = self._get_user_menu_record(user_id)
        if not record:
            return {"item_keys": []}

        elementos = record.get('elementos') or []
        item_keys = []
        for elemento in elementos:
            key = elemento.get(self.MENUS_CATALOG_OBJ_ID, {}).get(self.menu_form_fields['key'])
            key = self.unlist(key) if key else None
            if key:
                item_keys.append(key)
        return {"item_keys": item_keys}

    def save_user_menu_items(self, user_id, item_keys):
        """
        Reemplaza los items de menú asignados a un usuario (forma CONFIGURACION_MENUS).
        """
        details_menus = self.get_format_user_menus(filter_keys=item_keys) if item_keys else []
        elementos = []
        for menu in details_menus:
            elementos.append({
                f"{self.MENUS_CATALOG_OBJ_ID}": {
                    self.menu_form_fields['menu']: menu['menu'],
                    self.menu_form_fields['seccion']: menu['seccion'],
                    self.menu_form_fields['elemento']: menu['elemento'],
                    self.menu_form_fields['key']: [menu['key']],
                    self.menu_form_fields['plataforms']: [menu['plataforms']],
                }
            })

        user_data = None
        for user in self.list_users():
            if user['user_id'] == user_id:
                user_data = user
                break

        answers = {
            self.USUARIOS_OBJ_ID: {
                self.menu_form_fields['username']: user_data.get('username') if user_data else '',
                self.menu_form_fields['usuario_id']: [user_id],
            },
            self.menu_form_fields['elementos']: elementos,
        }

        metadata = self.lkf_api.get_metadata(form_id=self.MENUS_FORM)
        existing = self._get_user_menu_record(user_id)
        metadata['answers'] = answers
        if existing and existing.get('_id'):
            metadata['_id'] = existing['_id']
            res = self.net.patch_forms_answers(metadata)
        else:
            res = self.lkf_api.post_forms_answers(metadata)
        return res


if __name__ == "__main__":
    script_obj = Base(settings, sys_argv=sys.argv, use_api=True)
    script_obj.console_run()
    data = script_obj.data.get('data', {})

    option = data.get("option", '')
    payload = data.get("payload", {})
    items = data.get("items", [])
    record_id = data.get("record_id")
    user_id = data.get("user_id")
    item_keys = data.get("item_keys", [])

    dispatcher = {
        "list_menu_items": lambda: script_obj.list_menu_items(),
        "save_menu_item": lambda: script_obj.save_menu_item(payload),
        "save_menu_items_batch": lambda: script_obj.save_menu_items_batch(items),
        "replace_menu_catalog": lambda: script_obj.replace_menu_catalog(items),
        "delete_menu_item": lambda: script_obj.delete_menu_item(record_id),
        "list_users": lambda: script_obj.list_users(),
        "get_user_menu_items": lambda: script_obj.get_user_menu_items(user_id),
        "save_user_menu_items": lambda: script_obj.save_user_menu_items(user_id, item_keys),
    }

    action = dispatcher.get(option)
    if action:
        response = action()
    else:
        response = {"error": "Opción no válida"}

    script_obj.HttpResponse({"data": response})
