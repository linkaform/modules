# -*- coding: utf-8 -*-
import sys, simplejson
from bson import ObjectId


from lkf_addons.addons.base.app import CargaUniversal
from account_settings import *


FORM_ID_CARGA_UNIVERSAL = 132984
ESTATUS_FINALES = ('carga_terminada', 'error')


class CargaUniversalModule(CargaUniversal):

    def update_status_record(self, status, msg_comentarios='', record_ids=[], form_id=None):
        """
        - 'en_espera' del registro propio: se escribe directo en Mongo (sin patch,
          para no disparar reglas/workflows).
        - Cualquier otro caso usa la original (API). Si es un estatus final del
          registro propio, se libera el siguiente registro en espera.
        """
        if status == 'en_espera' and not record_ids and self.record_id:
            set_answers = {f"answers.{self.field_id_status}": status}
            self.current_record['answers'][self.field_id_status] = status
            if msg_comentarios:
                set_answers[f"answers.{self.field_id_comentarios}"] = msg_comentarios
                self.current_record['answers'][self.field_id_comentarios] = msg_comentarios
            return self.cr.update_one({"_id": ObjectId(self.record_id)}, {"$set": set_answers})

        res = super().update_status_record(status, msg_comentarios, record_ids, form_id)
        if status in ESTATUS_FINALES and not record_ids:
            self.liberar_siguiente_en_espera()
        return res

    def liberar_siguiente_en_espera(self):
        """Busca el registro en_espera más antiguo y le hace patch a 'cargar_documentos'."""
        query = {
            "form_id": FORM_ID_CARGA_UNIVERSAL,
            "deleted_at": {"$exists": False},
            f"answers.{self.field_id_status}": "en_espera",
        }
        if self.record_id:
            query["_id"] = {"$ne": ObjectId(self.record_id)}
        siguiente = self.cr.find_one(query, {"_id": 1, "folio": 1}, sort=[("created_at", 1)])
        if not siguiente:
            return None
        print(f"**** Liberando registro en espera folio {siguiente.get('folio')}")
        return self.lkf_api.patch_multi_record(
            answers={self.field_id_status: "cargar_documentos"},
            form_id=FORM_ID_CARGA_UNIVERSAL,
            record_id=[str(siguiente["_id"])],
        )


def buscar_workflow_corriendo(class_obj):
    """
    Regresa el doc de workflow_log de otra Carga Universal Running, o None.
    Se ignoran los Running cuyo registro ya tiene estatus final en form_answer
    (el log puede seguir en Running un instante después de terminar la carga).
    """
    record_query = {"$type": "objectId"}
    if class_obj.record_id:
        # Excluye la ejecución en curso de este mismo registro
        record_query["$ne"] = ObjectId(class_obj.record_id)
    query = {
        "form_id": FORM_ID_CARGA_UNIVERSAL,
        "workflow_response_content": "Running",
        "name": "Carga Universal",
        "record_id": record_query,
    }
    field_status = f"answers.{class_obj.field_id_status}"
    for wkf_doc in class_obj.cr_wkf.find(query, {"record_id": 1, "folio": 1, "workflow_record_folio": 1}):
        rec = class_obj.cr.find_one({"_id": wkf_doc["record_id"]}, {field_status: 1}) or {}
        if rec.get("answers", {}).get(class_obj.field_id_status) in ESTATUS_FINALES:
            continue
        return wkf_doc
    return None


def folio_en_ejecucion(class_obj, wkf_doc):
    """Obtiene el folio del registro que tiene la ejecución en curso."""
    folio = wkf_doc.get("folio") or wkf_doc.get("workflow_record_folio")
    if not folio:
        # Respaldo: leerlo del registro en form_answer
        rec = class_obj.cr.find_one({"_id": wkf_doc["record_id"]}, {"folio": 1}) or {}
        folio = rec.get("folio")
    return folio or str(wkf_doc["record_id"])


if __name__ == '__main__':
    class_obj = CargaUniversalModule(settings=settings, sys_argv=sys.argv, use_api=True)
    class_obj.console_run()
    wkf_running = buscar_workflow_corriendo(class_obj)
    if wkf_running:
        folio = folio_en_ejecucion(class_obj, wkf_running)
        print(f'**** Carga Universal en ejecución (folio {folio}), registro en espera')
        class_obj.update_status_record(
            'en_espera',
            msg_comentarios=f"Se encontró la ejecución con folio {folio} en proceso, este registro se ejecutará al terminar el actual"
        )
        sys.exit(0)
    resp_cu = class_obj.carga_doctos()
