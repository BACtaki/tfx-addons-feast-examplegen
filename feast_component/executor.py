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
from typing import Any, Dict, Optional, List

import apache_beam as beam
import feast
import tensorflow as tf
from apache_beam.options import value_provider
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from tfx.components.example_gen import base_example_gen_executor
from tfx.extensions.google_cloud_big_query import utils
from tfx.extensions.google_cloud_big_query.example_gen import executor
from tfx.proto import example_gen_pb2
from tfx import types

_REPO_CONFIG = "repo_conf"
_FEATURE_REFS_KEY = "feature_refs"
_FEATURE_VIEW_REF_KEY = "feature_view_ref"
_COMPUTE_STATS = "compute_stats"
_OUTPUT_FORMAT = "output_format"


def _custom_config_to_dict(custom_config):
    config = example_gen_pb2.CustomConfig()
    json_format.Parse(custom_config, config)
    s = Struct()
    config.custom_config.Unpack(s)
    config_dict = MessageToDict(s)
    return config_dict


class _BQConverter(executor._BigQueryConverter):
    """This can be used to add other formats needed."""


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.Pipeline)
@beam.typehints.with_output_types(tf.train.Example)
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
    custom_config = _custom_config_to_dict(exec_properties["custom_config"])

    features = custom_config[_FEATURE_REFS_KEY]
    feature_view_ref = custom_config[_FEATURE_VIEW_REF_KEY]
    out_format = custom_config.get(_OUTPUT_FORMAT, "tfexample")
    with tempfile.TemporaryDirectory() as t:
        with open(os.path.join(t, "feature_store.yaml"), "w") as f:
            f.write(custom_config[_REPO_CONFIG])

        fs = feast.FeatureStore(repo_path=t)

    retrieval_job = None
    if features:
        retrieval_job = fs.get_historical_features(
            entity_df=split_pattern, features=features
        )
    elif feature_view_ref:
        # ToDo: Add feature view ref implementation
        raise NotImplementedError("Feature view ref is not yet supported")
    else:
        raise RuntimeError("Either feature view or feature refs should be provided")

    sql = retrieval_job.to_sql()

    beam_pipeline_args = exec_properties["_beam_pipeline_args"]
    pipeline_options = beam.options.pipeline_options.PipelineOptions(beam_pipeline_args)
    # Try to parse the GCP project ID from the beam pipeline options.
    project = pipeline_options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions
    ).project
    if isinstance(project, value_provider.ValueProvider):
        project = project.get()
    converter = _BQConverter(sql, project)
    map_function = None

    if out_format == "tfexample":
        map_function = converter.RowToExample
    else:
        raise NotImplementedError(f"Format {out_format} is not currently supported")
    return (
        pipeline
        | "QueryTable" >> utils.ReadFromBigQuery(query=sql)
        | "ToTFExample" >> beam.Map(map_function)
    )


class Executor(base_example_gen_executor.BaseExampleGenExecutor):
    """Generic TFX BigQueryExampleGen executor."""

    def Do(
        self,
        input_dict: Dict[str, List[types.Artifact]],
        output_dict: Dict[str, List[types.Artifact]],
        exec_properties: Dict[str, Any],
    ) -> None:
        custom_config = _custom_config_to_dict(exec_properties["custom_config"])
        compute_stats = custom_config.get(_COMPUTE_STATS, False)
        if compute_stats:
            # ToDo: Compute statistics here for either unique split or configured split
            raise NotImplementedError("Statistics it not implemented yet")

        return super().Do(input_dict, output_dict, exec_properties)

    def GetInputSourceToExamplePTransform(self) -> beam.PTransform:
        """Returns PTransform for BigQuery to TF examples."""
        return _FeastToExampleTransform
