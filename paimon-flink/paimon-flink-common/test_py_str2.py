from pyflink.table.udf import udf
from pyflink.table import DataTypes
@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
def eval(str):
    return f"{str}11"
