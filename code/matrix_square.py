from odps.udf import annotate
import numpy as np


@annotate("string->string")
class matrix_square(object):

   def evaluate(self, arg1):
        t = arg1.split(",")

        Xa = np.array([t] , dtype='float64')
        XaT = Xa.T
        Aa_factor = np.dot(XaT, Xa)

        str(Aa_factor.flatten())

        return str(Aa_factor.flatten().tolist())