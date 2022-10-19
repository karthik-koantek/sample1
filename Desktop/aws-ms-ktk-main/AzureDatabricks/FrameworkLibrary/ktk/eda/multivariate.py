from .eda import EDA

class Multivariate(EDA):
    def __init__(self, df, tableName):
        super().__init__(df, tableName)