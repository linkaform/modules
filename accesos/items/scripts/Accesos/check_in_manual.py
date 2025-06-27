# coding: utf-8
import sys, simplejson, json
from linkaform_api import settings
from account_settings import *

from accesos_utils import Accesos

class Accesos(Accesos):

    def __init__(self, settings, folio_solicitud=None, sys_argv=None, use_api=False, **kwargs):
        #--Variables
        # Module Globals#
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api, **kwargs)
        self.load(module='Location', **self.kwargs)
        
        self.f.update({
            'option_checkin': '663bffc28d00553254f274e0',
            'image_checkin': '6855e761adab5d93274da7d7',
            'comment_checkin': '66a5b9bed0c44910177eb724',
        })

if __name__ == "__main__":
    acceso_obj = Accesos(settings, sys_argv=sys.argv)
    acceso_obj.console_run()
    print('ANSWERSSSSSSSSSSSSS', simplejson.dumps(acceso_obj.answers, indent=3))

    location = acceso_obj.answers.get(acceso_obj.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID, {}).get(acceso_obj.Location.f['location'], '')
    area = acceso_obj.answers.get(acceso_obj.CONF_AREA_EMPLEADOS_AP_CAT_OBJ_ID, {}).get(acceso_obj.mf['nombre_area_salida'], '')
    image = acceso_obj.answers.get(acceso_obj.f['image_checkin'], '')
    comment = acceso_obj.answers.get(acceso_obj.f['comment_checkin'], '')
    option = acceso_obj.answers.get(acceso_obj.f['option_checkin'], '')
    
    response = {}
    if option == 'iniciar_turno':
        response = acceso_obj.do_checkin(location, area, check_in_manual={
            'image': image,
            'comment': comment
        })
    elif option == 'cerrar_turno':
        load_shift = acceso_obj.get_shift_data(booth_location=location, booth_area=area)
        checkin_id = load_shift.get('booth_status', {}).get('checkin_id', '')
        
        if checkin_id:
            response = acceso_obj.do_checkout(checkin_id=checkin_id, location=location, area= area)

    sys.stdout.write(simplejson.dumps({
        'status': 101,
        'response': response
    }))