# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generic TFX FeastExampleGen executor.
"""

import os
import tempfile
from typing import Any, Dict, Optional

import apache_beam as beam
import feast
from apache_beam.options import value_provider
from feast.infra.offline_stores.bigquery import BigQueryRetrievalJob
from feast.infra.offline_stores.offline_store import RetrievalJob
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from tfx.components.example_gen import base_example_gen_executor
from tfx.extensions.google_cloud_big_query import utils
from tfx.proto import example_gen_pb2

from . import converters

_REPO_CONFIG_KEY = "repo_conf"
_FEATURE_KEY = "feature_refs"
_FEATURE_SERVICE_KEY = "feature_service_ref"
_OUTPUT_FORMAT = "output_format"


def _load_custom_config(custom_config):
    config = example_gen_pb2.CustomConfig()
    json_format.Parse(custom_config, config)
    s = Struct()
    config.custom_config.Unpack(s)
    config_dict = MessageToDict(s)
    return config_dict


def _load_feast_feature_store(custom_config: Dict[str, Any]) -> feast.FeatureStore:
    with tempfile.TemporaryDirectory() as t:
        with open(os.path.join(t, "feature_store.yaml"), "w") as f:
            f.write(custom_config[_REPO_CONFIG_KEY])

    return feast.FeatureStore(repo_path=t)


def _get_retrieval_job(
    entity_query: str, custom_config: Dict[str, Any]
) -> RetrievalJob:
    """Get feast retrieval job

    Args:
        entity_query (str): entity query.
        custom_config (Dict[str, Any]): Custom configuration from component

    Raises:
        RuntimeError: [description]

    Returns:
        RetrievalJob: [description]
    """
    feature_list = custom_config[_FEATURE_KEY]
    feature_service = custom_config[_FEATURE_SERVICE_KEY]
    fs = _load_feast_feature_store(custom_config)

    if feature_list:
        features = feature_list
    elif feature_service:
        features = fs.get_feature_service(feature_service)
    else:
        raise RuntimeError("Either feature service or feature list should be provided")

    return fs.get_historical_features(entity_df=entity_query, features=features)


def _get_gcp_project(exec_properties: Dict[str, Any]) -> Optional[str]:
    # Get GCP project from exec_properties.
    beam_pipeline_args = exec_properties["_beam_pipeline_args"]
    pipeline_options = beam.options.pipeline_options.PipelineOptions(beam_pipeline_args)
    # Try to parse the GCP project ID from the beam pipeline options.
    project = pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions
    ).project
    if isinstance(project, value_provider.ValueProvider):
        return project.get()
    return None


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.Pipeline)
@beam.typehints.with_output_types(bytes)
def _FeastToExampleTransform(
    pipeline: beam.Pipeline, exec_properties: Dict[str, Any], split_pattern: str
) -> beam.pvalue.PCollection:
    """Read from BigQuery and transform to TF examples.

    Args:
      pipeline: beam pipeline.
      exec_properties: A dict of execution properties.
      split_pattern: Split.pattern in Input config, a BigQuery sql string.

    Returns:
      PCollection of TF examples.
    """
    # Load custom config dictionary
    custom_config = _load_custom_config(exec_properties["custom_config"])

    # Get Feast retrieval job
    retrieval_job = _get_retrieval_job(
        entity_query=split_pattern, custom_config=custom_config
    )

    # Setup datasource and converter.
    if isinstance(retrieval_job, BigQueryRetrievalJob):
        query = retrieval_job.to_sql()
        datasource = utils.ReadFromBigQuery(query=query)
        converter = converters._BQConverter(query, _get_gcp_project(exec_properties))
    else:
        raise NotImplementedError(
            f"Support for {type(retrieval_job)} is not available yet. For now we only support BigQuery source."
        )

    # Setup converter from dictionary of str -> value to bytes
    map_function = None
    out_format = custom_config.get(_OUTPUT_FORMAT, "tfexample")
    if out_format == "tfexample":
        map_function = converter.RowToExampleBytes
    else:
        raise NotImplementedError(
            f"Format {out_format} is not currently supported. Currently we only support tfexample"
        )

    # Setup pipeline
    return (
        pipeline
        | "DataSource" >> datasource
        | f"To{out_format.capitalize()}Bytes" >> beam.Map(map_function)
    )


class Executor(base_example_gen_executor.BaseExampleGenExecutor):
    """Generic TFX FeastExampleGen executor."""

    def GetInputSourceToExamplePTransform(self) -> beam.PTransform:
        """Returns PTransform for BigQuery to TF examples."""
        return _FeastToExampleTransform
