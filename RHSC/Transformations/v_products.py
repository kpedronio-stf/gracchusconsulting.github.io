class v_products:
    def __init__(self, df,table):
        self.df = df
        self.table = table

    def add_col(self):
        self.df = self.df.withColumn('product',lit(self.table))
    def transform(self):
        self.add_col()
        return self.df