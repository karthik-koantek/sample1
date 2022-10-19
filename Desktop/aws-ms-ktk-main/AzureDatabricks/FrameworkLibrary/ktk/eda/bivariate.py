from .eda import EDA

class Bivariate(EDA):
    def __init__(self, df, tableName, col, col2):
        super().__init__(df, tableName)
        self.col = col
        self.col2 = col2
    
    def jointPlot(self):
        import seaborn as sns
        return sns.jointplot(x=self.col, y=self.col2, data=self.pdf)

    def disPlot(self):
        import seaborn as sns
        return sns.displot(self.pdf,x=self.col,  y=self.col2) 

    def relPlot(self):
        import seaborn as sns
        return sns.relplot(x=self.col, y=self.col2, data=self.pdf) 

    def relPlotLine(self):
        import seaborn as sns
        return sns.relplot(x=self.col, y=self.col2, kind="line",  data=self.pdf)

    def plotHeatmap(self):
        import seaborn as sns 
        import matplotlib.pyplot as plt 
        plt.figure(figsize=(25,12))
        sns.heatmap(self.pdf.corr(), annot=True)
        return plt.show()
    
    def plotBivariate(self):
        import dataprep.eda as dataprep_eda
        return dataprep_eda.plot(self.pdf, self.col, self.col2)
    
    def plotBivariateCorrelations(self):
        import dataprep.eda as dataprep_eda 
        return dataprep_eda.plot_correlation(self.pdf, self.col, self.col2)