from .eda import EDA

class Univariate(EDA):
    def __init__(self, df, tableName, col):
        super().__init__(df, tableName)
        self.col = col
    
    def getNullValuesSum(self):
        return self.pdf[self.col].isnull().sum()
    
    def getValueCounts(self):
        return self.pdf[self.col].value_counts(dropna=False)
    
    def plotHistogram(self):
        import matplotlib.pyplot as plt
        plt.hist(self.pdf[self.col])
        plt.show()
    
    def plotScatter(self):
        import matplotlib.pyplot as plt
        plt.scatter(self.pdf.index, self.pdf[self.col])
        plt.show()

    def getUnivariate(self):
        nullVals = self.getNullValuesSum()
        valueCounts = self.getValueCounts()
        return nullVals, valueCounts
    
    def displayUnivariate(self):
        nullVals, valueCounts = self.getUnivariate()
        print(f'\nUnivariate Analysis for column : {self.col} \n')
        print(f'\nNull Values : {nullVals} \n')
        print(f'\nValue Counts : {valueCounts} \n')
        self.plotHistogram()
        self.plotScatter()
    
    def plotUnivariate(self):
        import dataprep.eda as dataprep_eda
        return dataprep_eda.plot(self.pdf, self.col)

    def plotUnivariateCorrelations(self):
        import dataprep.eda as dataprep_eda
        return dataprep_eda.plot(self.pdf, self.col)