# -*- coding: utf-8 -*-
#####
#####
# Script para regresar query de mongo
#
#
#####
# coding: utf-8
import simplejson


def get_query():
    query = {
        "collection":"form_answer",
        "db_name":"infosync_answers_client_15667",
        "query" :[
                {"$match":{
                        "form_id":119972, "deleted_at":{"$exists":False}
                    }
                    },
                    {"$unwind":"$answers.c00000000000000000000003"},
                    {"$project":
                        {
                           "folio": "$folio",
                           "fecha_creacion":"$created_at",
                           "empleado":"$created_by_name",
                           "numero_orden": "$answers.672e8edfd2f1f131c66505d9",
                           "fecha_de_apertura": "$answers.666a160967bcb9e23b1cc963",
                           "fecha_de_atencion": "$answers.666a160967bcb9e23b1cc964",
                           "logar_de_trabajo": "$answers.667da18cb8c88bca5ab4f295",
                           "razon_social": "$answers.670f50c71f4102516864159d.6687f2f37b2c023e187d6252",
                           "fecha_desde":"$answers.c00000000000000000000003.c00000000000000000000004",
                           "fecha_hasta":"$answers.c00000000000000000000003.c00000000000000000000005",
                           "year":{"$year":"$created_at"},
                         }
                     }
                ],
        "command": "aggregate",
        "selectColumns":[],
        "input_schema":  [
          { "folio": "text" },
          { "fecha_creacion": "timestamp" },
          { "empleado": "text" },
          { "numero_orden": "text" },
          { "fecha_de_apertura": "timestamp" },
          { "fecha_de_atencion": "timestamp" },
          { "logar_de_trabajo": "text" },
          { "razon_social": "text" },
          { "fecha_desde": "timestamp" },
          { "fecha_hasta": "timestamp" },
          { "year": "integer" },
        ],
    "output_schema":
        [
          { "folio": "text" },
          { "fecha_creacion": "timestamp" },
          { "empleado": "text" },
          { "numero_orden": "text" },
          { "fecha_de_apertura": "timestamp" },
          { "fecha_de_atencion": "timestamp" },
          { "logar_de_trabajo": "text" },
          { "razon_social": "text" },
          { "fecha_desde": "timestamp" },
          { "fecha_hasta": "timestamp" },
          { "year": "integer" },
        ]
    }
    return query

if __name__ == "__main__":
    query = get_query()
    print(simplejson.dumps(query))
