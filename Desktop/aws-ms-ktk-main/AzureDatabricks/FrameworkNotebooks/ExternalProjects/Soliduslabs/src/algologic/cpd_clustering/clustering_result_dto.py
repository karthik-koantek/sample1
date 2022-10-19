# Databricks notebook source
from typing import List
import json


class ClusterFeatureStats:
    def __init__(self, name: str, std: float, mean: float):
        self.name = name
        self.std = std
        self.mean = mean

    def __str__(self):
        return self.__dict__

    def __repr__(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class ClusterInfo:
    def __init__(self, label: str, size: int, features: List[ClusterFeatureStats]):
        self.label = label
        self.size = size
        self.features = features

    def __str__(self):
        return self.__dict__

    def __repr__(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class ClusteringSegmentResults:
    def __init__(self, id_type: str, solidus_client: str, segment_id: int, is_default_segment: bool,
                 standardization_values: List[ClusterFeatureStats], clusters: List[ClusterInfo]):
        self.id_type = id_type
        self.solidus_client = solidus_client
        self.is_default_segment = is_default_segment
        self.segment_id = segment_id
        self.standardization_values = standardization_values
        self.clusters = clusters

    def __str__(self):
        return self.__dict__

    def __repr__(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class ClusterIdentifierResults:
    def __init__(self, solidus_client: str, id_type: str, days_back: int,
                 segment_data: List[ClusteringSegmentResults]):
        self.id_type = id_type
        self.segments = segment_data
        self.solidus_client = solidus_client
        self.days_back = days_back

    def __str__(self):
        return self.__dict__

    def __repr__(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
