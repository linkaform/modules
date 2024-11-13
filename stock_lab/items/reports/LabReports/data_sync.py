import sys, simplejson
from stock_report import Reports
from linkaform_api import settings, network, utils
from lab_harvest_report import insert_tableau_data, get_report


def get_query():
    query = {
        "collection": "Tableau",
        "db_name": "infosync_answers_client_9908",
        "query": [
            {
                "$project": {
                    "_id": 0,
                    "from": "$from",
                    "harvest_year": "$harvest_year",
                    "harvest_month": "$harvest_month", 
                    "harvest_week": "$harvest_week",
                    "plant_code": "$plant_code",
                    "total": "$total"
                }
            }
        ],
        "command": "find",
        "selectColumns": [],
        "input_schema": [
            {"_id": "text"},
            {"from": "text"},
            {"harvest_year": "int"},
            {"harvest_month": "int"},
            {"harvest_week": "int"},
            {"plant_code": "text"},
            {"total": "double"}
        ],
        "output_schema": [
            {"_id": "text"},
            {"_from": "text"},
            {"harvest_year": "int"},
            {"harvest_month": "int"},
            {"harvest_week": "int"},
            {"plant_code": "text"},
            {"total": "double"}
        ]
    }
    return query


if __name__ == "__main__":
    report_obj = Reports(settings, sys_argv=sys.argv, use_api=True)
    data = get_report(report_obj)
    insert_tableau = insert_tableau_data(report_obj, data)
    query = get_query()