# -*- coding: utf-8 -*-
from datetime import datetime
from linkaform_api import base
from lkf_addons.addons.calidad.app import Calidad
from PIL import Image
import numpy as np
from bson import ObjectId

from linkaform_api import settings, utils
from account_settings import *

class Calidad(Calidad):
    print('Entra a calidad utils')

    def __init__(self, settings, sys_argv=None, use_api=False):
        super().__init__(settings, sys_argv=sys_argv, use_api=use_api)
        
        # FORMS
        self.REPORTE_GRAFICO = self.lkm.form_id('reporte_grfico', 'id')
        self.INSPECCION_CALIDAD = self.lkm.form_id('inspeccion', 'id')
        
        # CATALOGS
        self.PROCESOS_Y_MAQ_RELACIONADAS = self.lkm.catalog_id('procesos_y_maquinas_relacionadas')
        self.PROCESOS_Y_MAQ_RELACIONADAS_ID = self.PROCESOS_Y_MAQ_RELACIONADAS.get('id')
        self.PROCESOS_Y_MAQ_RELACIONADAS_OBJ_ID = self.PROCESOS_Y_MAQ_RELACIONADAS.get('obj_id')
        
        self.PRODUCTOS_CALIDAD = self.lkm.catalog_id('productos')
        self.PRODUCTOS_CALIDAD_ID = self.PRODUCTOS_CALIDAD.get('id')
        self.PRODUCTOS_CALIDAD_OBJ_ID = self.PRODUCTOS_CALIDAD.get('obj_id')
        
        # FIELDS
        self.f.update({
            'parametro_de_medicion': 'd00000000000000000000001',
            'lsl': 'd00000000000000000000021',
            'usl': 'd00000000000000000000022',
            'target': 'd00000000000000000000024',
            'media': 'd00000000000000000000002',
            'mediana': 'd00000000000000000000003',
            'desviacion_estandar': 'd00000000000000000000004',
            'cp': 'd00000000000000000000005',
            'cpu': 'd00000000000000000000006',
            'cpl': 'd00000000000000000000007',
            'cpk': 'd00000000000000000000008',
            'tamano_muestra_n': 'd00000000000000000000009',
            'campo_grafico': 'd00000000000000000000010',
            'status_reporte': 'a00000000000000000000003',
            'observaciones_reporte': 'a00000000000000000000004',
            'nombre_producto_calidad': '00000ffff717042907171594',
            'cantidad_producida_c1': '60ccc2fda915e60a9e175560',
            'cantidad_producida_c2': '60cbcf6092841ffc25ca864a',
            'cantidad_producida_c3': '60ccc5173785a8b10c17553d',
            'cantidad_producida_c4': '60ccc70a58195a4d5beb1188',
            'cantidad_producida_c5': 'aaaaaf6092841ffc25ca863d',
            'existe_alguna_falla': '6104630669843e373836fcbc',
            'cantidad_producida_grafico': 'a00000000000000000000005',
            'fallas_grafico': 'a00000000000000000000006',
            'fecha_hora_grupo_datos': 'd000000000000000000000b1',
            'valor_grupo_datos': 'd000000000000000000000b2',
            'auditor_grupo_datos': 'd000000000000000000000b3',
            'fecha_inicio_reporte': 'f00000000000000000000001',
            'fecha_fin_reporte': 'f00000000000000000000002',
            'parametro_de_medicion': 'd10000000000000000000001',
            'grupo_datos': 'd000000000000000000000b0',
        })
        
    def get_limits(self, plt, contorl_limit, n, color=None, linestyle=None):
        limit_x = np.array([0, n])
        limit_y = np.array([contorl_limit, contorl_limit])
        plt.plot(limit_x, limit_y, color=color, linestyle=linestyle)
        return plt

    def Cp(self, mylist, usl, lsl):
        arr = np.array(mylist)
        arr = arr.ravel()
        sigma = np.std(arr)
        if sigma:
            Cp = float(usl - lsl) / (6*sigma)
        else:
            Cp = 0
        return Cp

    def Cpk(self, mylist, usl, lsl):
        arr = np.array(mylist)
        arr = arr.ravel()
        sigma = np.std(arr)
        m = np.mean(arr)
        if sigma:
            Cpu = float(usl - m) / (3*sigma)
            Cpl = float(m - lsl) / (3*sigma)
            Cpk = np.min([Cpu, Cpl])
            return Cpu, Cpl, Cpk
        else:
            return 0, 0, 0

    def get_graph_info(self, parameter, data, form_id):
        lkf_api = utils.Cache(settings)
        print('== parameter', parameter)
        print('== data',data)
        #parameter : {name:txt, lsl: float, usl:float, target:float}
        n = len(data)
        #TODO get lsl, uls, target de cada paremetro del producto
        #
        #lower limit
        lsl = parameter.get('lsl',0)
        #upper limit
        usl = parameter.get('usl',0)
        #target
        target = parameter.get('target',0)

        cp= self.Cp(data, usl, lsl)
        cpu, cpl, cpk = self.Cpk(data, usl, lsl)
        #media

        data_points = np.array(data)
        mean = data_points.mean()
        median = np.median(data_points)
        sigma = data_points.std()

        # ESTO SI SE DEBE PONER DE FORMA ESTANDAR
        res = {
            self.f['parametro_de_medicion']: parameter.get('name',''),
            self.f['lsl']: round(parameter.get('lsl',0), 4),
            self.f['usl']: round(parameter.get('usl',0), 4),
            self.f['target']: round(parameter.get('target',0), 4),
            self.f['media']: round(mean, 4),
            self.f['mediana']: round(median, 4),
            self.f['desviacion_estandar']: round(sigma, 4),
            self.f['cp']: round(cp, 4),
            self.f['cpu']: round(cpu, 4),
            self.f['cpl']: round(cpl, 4),
            self.f['cpk']: round(cpk, 4),
            self.f['tamano_muestra_n']: round(n, 4)
        }

        #revisar porque si no lo importo aqui se queja
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()

        plt.plot(data_points, marker = 'o')
        #sets Upper Limit
        plt = self.get_limits(plt, usl, n, color='orange')
        #sets lower limit
        plt = self.get_limits(plt, lsl, n,  color='orange')
        #sets lower target
        plt = self.get_limits(plt, target, n,  color='green')
        #sets mean
        plt = self.get_limits(plt, mean, n,  color='#871B89' ,linestyle='dashed')
        #Poner Numero de Orden de Produccion o Producto
        #605548e4d8fcccc640e62897.605548e4d8fcccc640e62898
        #title = "Codigo de Producto"
        #a00000000000000000000002
        title = "Orden de Produccion {}".format( parameter.get('name', '') )

        plt.title(title)
        plt.xlabel("Num. de Muestras")
        plt.ylabel( parameter.get('name', '') )
        plt.grid()

        #textos
        textstr = '\n'.join((
            r'$\mu=%.2f$' % (mean, ),
            r'$\mathrm{median}=%.2f$' % (median, ),
            r'$\sigma=%.2f$' % (sigma, )))

        textstr_cp = '\n'.join((
            'c=%.2f' % (cp ),
            'cpu=%.2f' % (cpu ),
            'cpl=%.2f' % (cpl ),
            'cpk=%.2f' % (cpk )))

        props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)

        plt.text(0.05, 0.95, textstr, transform=ax.transAxes, fontsize=14,
                verticalalignment='top', bbox=props)

        plt.text(.85, .05, textstr_cp, transform=ax.transAxes, fontsize=14,
                verticalalignment='bottom', bbox=props)

        graph_path = '/tmp/{}.png'.format(str(ObjectId()))
        #print('graph_path=', graph_path)
        plt.savefig(graph_path)
        img = Image.open(graph_path)
        rgb_img = img.convert('RGB')
        rgb_img.save(graph_path.replace('.png','.jpg'))

        # Subiendo la imagen a blackblaze
        rgb_path = graph_path.replace('.png','.jpg')
        rb_file = open(rgb_path, 'rb')
        img_file = {'File': rb_file}
        upload_data = {'form_id': form_id, 'field_id': self.f['campo_grafico']}
        upload_url = lkf_api.post_upload_file(data=upload_data, up_file=img_file, jwt_settings_key='USER_JWT_KEY')
        #print('++++++++ upload_url={}'.format(upload_url))
        img_uploaded = {}
        if upload_url.get('data', False):
            img_uploaded.update({
                'file_name': upload_url.get('data', {}).get('file_name', ''),
                'file_url': upload_url.get('data', {}).get('file', '')
            })
        if img_uploaded:
            res.update({self.f['campo_grafico']: img_uploaded})
        return res