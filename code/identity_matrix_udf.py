from odps.udf import annotate
import numpy as np


@annotate("bigint->string")
class identity_matrix(object):

   def evaluate(self, arg1):
        Xa = np.eye(arg1).flatten().tolist()

        tmp_array = [str(i) for i in Xa]

        output = ','.join(tmp_array)

        return output