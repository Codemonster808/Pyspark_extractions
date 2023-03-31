
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, round

class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("ElectricChargePointsETLJob")
                                          .getOrCreate())

    def extract(self):
        df = (self.spark_session.read
                                .option('header', 'true')
                                .csv(self.input_path))
        return df

    def transform(self, df):
        df = (df.groupBy('CPID')
                .agg(round(max('PluginDuration'), 2).alias('max_duration'),
                     round(avg('PluginDuration'), 2).alias('avg_duration'))
                .withColumnRenamed('CPID', 'chargepoint_id'))
        return df

    def load(self, df):
        (df.write
            .option('compression', 'snappy')
            .parquet(self.output_path))

    def run(self):
        self.load(self.transform(self.extract()))