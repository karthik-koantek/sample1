#import pandas as pd 
#import numpy as np
# import ipywidgets as widgets
# from ipywidgets import interact, interact_manual
#import seaborn as sns
#import matplotlib.pyplot as plt
#import missingno as msno
# import dataprep.eda as dataprep_eda
# from dataprep.datasets import load_dataset

class EDA:
    def __init__(self, df, tableName):
        self.df = df
        self.pdf = df.toPandas()
        self.catalogName, self.tableName = tableName.split(".")
        self.ContinuousColumns = self.getContinuousColumns()
        self.CategoricalColumns = self.getCategoricalColumns()
    
    def getContinuousColumns(self, useSpark=True):
        if useSpark == False:
            import numpy as np
            continuousColumns = list(self.pdf.select_dtypes(include=[np.number]).columns)
        else:
            continuousColumns = [c[0] for c in self.df.dtypes if c[1] in ["double", "float"]]
        return continuousColumns
    
    def getCategoricalColumns(self, useSpark=True):
        if useSpark == False:
            import numpy as np
            categoricalColumns = list(self.pdf.select_dtypes(exclude=[np.number]).columns)
        else:
            categoricalColumns = [c[0] for c in self.df.dtypes if c[1] not in ["double", "float"]]
        return categoricalColumns

    