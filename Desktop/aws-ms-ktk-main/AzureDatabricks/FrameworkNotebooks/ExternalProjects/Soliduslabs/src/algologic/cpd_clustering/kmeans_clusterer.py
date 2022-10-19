# Databricks notebook source
from pyspark.ml.clustering import KMeans

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.ml.linalg import DenseVector

from pyspark import StorageLevel
from pyspark.broadcast import Broadcast

from math import sqrt, inf


# TODO: revisit "loss_derivative_cutoff" after moving to avg of loss
class KmeansAutoKClusterer:
    def __init__(
            self, spark, max_k: int = 30,
            loss_derivative_cutoff: float = 0.75, skip_k_step_size: int = 2, logger=None):
        # K might be lower than max_k, but never higher
        self.spark = spark
        self.max_k = max_k
        self.skip_k_step_size = skip_k_step_size
        self.logger = logger

        # Stop trying out more K-values once the loss-rate drops below this value
        # We use the "elbow-method" as explained here:
        # https://en.wikipedia.org/wiki/Elbow_method_(clustering)
        self.loss_derivative_cutoff = loss_derivative_cutoff

    # Note: We assume the feature column is standardized!
    def execute(self, df: DataFrame, features_colname: str, output_colname: str):

        self._log_info('Persisting to disk for enhanced performance...')
        df.persist(StorageLevel.MEMORY_AND_DISK_2)

        # Initializing variables to hold best performing clusters so far...
        prev_loss = float(inf)
        prev_k = None
        result = None

        for k in self._compose_k_values(int(self.max_k), int(self.skip_k_step_size)):
            self._log_info(f'Clustering for K={k}')
            kmeans = KMeans().setK(k).setSeed(1)
            kmeans.setFeaturesCol(features_colname)
            kmeans.setPredictionCol(output_colname)

            model = kmeans.fit(df)

            # Broadcasting cluster centers to the Spark-nodes, to be used in UDFs
            cluster_centers = model.clusterCenters()
            bcast_cluster_centers = self.spark.sparkContext.broadcast(cluster_centers)

            predictions = model.transform(df)

            self._log_info(f'Calculating loss for K={k}')
            predictions_and_loss = predictions.withColumn('loss',
                                                          self._calculate_euclidean_distance(bcast_cluster_centers)(
                                                              f.col('features_standard'), f.col('predictions')))
            loss_stats = \
                predictions_and_loss\
                .agg(f.sum('loss').alias('loss'), f.count('identifier').alias('counter')).collect()[0]

            loss_amount = loss_stats[0]
            loss_counter = loss_stats[1]
            self._log_info(f'loss stats: {loss_stats}')

            # TODO: If improvement rate below 7.5, don't do the new clusters (test should stop at 5!)
            improvement = (prev_loss - loss_amount) / loss_counter
            self._log_info(f'improvement_rate: {prev_loss - loss_amount} / {loss_counter} = {improvement}. '
                           f'Cutoff set to: {self.loss_derivative_cutoff}')

            prev_loss = loss_amount
            prev_k = k
            result = predictions

            if improvement < self.loss_derivative_cutoff:
                self._log_info(
                    f'Improvement rate ({improvement}) below cuttoff threshold ({self.loss_derivative_cutoff}). '
                    f'Stopping at K={prev_k}')
                return model, result

        self._log_info(f'max iterations (max-k) has been reached. Stopping at K={prev_k}')

        return model, result

    # Calculate distance between cluster-center & data-point using Spark-broadcasted list of cluster-centers
    # (In order to get the be used in the UDF)
    def _calculate_euclidean_distance(self, bcast_cluster_centers: Broadcast):
        def _helper_with_broadcast(data_point: DenseVector, cluster_label: int):
            cluster_center = bcast_cluster_centers.value[cluster_label]
            zipped = zip(data_point, cluster_center)
            return sqrt(sum([abs(x - y) ** 2 for x, y in zipped]))

        return f.udf(_helper_with_broadcast)

    def _compose_k_values(self, max_k: int, step_size: int):
        return [2] + [i for i in range(3, max_k, step_size)]

    def _log_info(self, msg):
        print(f'KmeansAutoKClusterer - {msg}')
        if self.logger:
            self.logger.info(f'KmeansAutoKClusterer - {msg}')
