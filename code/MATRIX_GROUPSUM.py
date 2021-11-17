from odps.udf import annotate
from odps.udf import BaseUDAF
import numpy as np
import re


@annotate('string,string->string')
class score_calculate(BaseUDAF):

    def new_buffer(self):
        return [[]] 

    def iterate(self, buffer, score, ratio):
        if None in (score,ratio):
            return None
        # tmp = np.array([float(re.findall(r"\d+\.?\d*", i)[0]) for i in ratio.split(',')])
        buffer[0].append(ratio)
        # buffer[0] += tmp
        # buffer[0] += (float(tmp.sum()) * float(tmp.sum()))

    def merge(self, buffer, pbuffer):

        # tmp2 = np.array([float(re.findall(r"\d+\.?\d*", i)[0]) for i in pbuffer[0].split(',')])
        # buffer[0] += tmp2
        buffer[0] += pbuffer[0]

    def terminate(self, buffer):
        list_array = np.array([[float(re.findall(r"\-?\d+\.?\d*", i)[0]) for i in mylist.split(',')] for mylist in buffer[0] if mylist!=''])
         # str(list_array.sum(axis=0).tolist())
        tmp_array = list_array.sum(axis=0).tolist()
        tmp_array = [str(i) for i in tmp_array]

        output = ','.join(tmp_array)
       
        return output