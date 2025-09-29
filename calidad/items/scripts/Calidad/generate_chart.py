# -*- coding: utf-8 -*-
from PIL import Image
import numpy as np
from bson import ObjectId

from linkaform_api import settings, utils
from account_settings import *

class Lkf_Chart():

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
                'd00000000000000000000001':parameter.get('name',''),
                'd00000000000000000000021':round(parameter.get('lsl',0), 4),
                'd00000000000000000000022':round(parameter.get('usl',0), 4),
                'd00000000000000000000024':round(parameter.get('target',0), 4),
                'd00000000000000000000002':round(mean, 4),
                'd00000000000000000000003':round(median, 4),
                'd00000000000000000000004':round(sigma, 4),
                'd00000000000000000000005':round(cp, 4),
                'd00000000000000000000006':round(cpu, 4),
                'd00000000000000000000007':round(cpl, 4),
                'd00000000000000000000008':round(cpk, 4),
                'd00000000000000000000009':round(n, 4)
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
        upload_data = {'form_id': form_id, 'field_id': 'd00000000000000000000010'}
        upload_url = lkf_api.post_upload_file(data=upload_data, up_file=img_file, jwt_settings_key='USER_JWT_KEY')
        #print('++++++++ upload_url={}'.format(upload_url))
        img_uploaded = {}
        if upload_url.get('data', False):
            img_uploaded.update({
                'file_name': upload_url.get('data', {}).get('file_name', ''),
                'file_url': upload_url.get('data', {}).get('file', '')
            })
        if img_uploaded:
            res.update({'d00000000000000000000010': img_uploaded})
        return res
