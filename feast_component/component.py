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
TFX FeastExampleGen Component Definition

QueryBasedExampleGen
https://github.com/tensorflow/tfx/blob/master/tfx/components/example_gen/component.py

PrestoExampleGen extends QueryBasedExampleGen
https://github.com/tensorflow/tfx/blob/master/tfx/examples/custom_components/presto_example_gen/presto_component/component.py

"""

import os
import pathlib
import tempfile
from typing import List, Optional, Union

import feast
from google.protobuf.struct_pb2 import Struct
from tfx.components.example_gen import component, utils
from tfx.dsl.components.base import executor_spec
from tfx.extensions.google_cloud_big_query.example_gen import executor
from tfx.orchestration import data_types
from tfx.proto import example_gen_pb2

from . import executor, spec


class FeastExampleGen(component.QueryBasedExampleGen):

    EXECUTOR_SPEC = executor_spec.BeamExecutorSpec(executor.Executor)
    SPEC_CLASS = spec.QueryBasedExampleGenWithStatsSpec

    def __init__(
        self,
        repo_config: feast.RepoConfig,
        entity_query: Optional[str] = None,
        feature_refs: Optional[List[str]] = None,
        feature_view_ref: Optional[str] = None,
        input_config: Optional[
            Union[example_gen_pb2.Input, data_types.RuntimeParameter]
        ] = None,
        compute_stats: bool = False,
        **kwargs
    ):
        """FeastExampleGen is a way for loading offline features. For now we support BQ.

        Args:
            repo_config (feast.RepoConfig): Feast repo configuration object
            entity_query (Optional[str], optional): Query used to obtain the entity dataframe. Defaults to None.
            feature_refs (Optional[List[str]], optional): List of features to retrieve as part of this job. Defaults to None.
            feature_view_ref (Optional[str], optional): [description]. Defaults to None.
            input_config (Optional[ Union[example_gen_pb2.Input, data_types.RuntimeParameter] ], optional): [description]. Defaults to None.
            compute_stats (bool, optional): [description]. Defaults to False.

        Raises:
            RuntimeError: [description]
            RuntimeError: [description]
        """
        if bool(entity_query) == bool(input_config):
            raise RuntimeError("Exactly one of query and input_config should be set.")
        input_config = input_config or utils.make_default_input_config(entity_query)

        if feature_view_ref and feature_refs:
            raise RuntimeError(
                "Exactly one of feature_view_ref and feature_refs should be set."
            )
        
        # ToDo: Potentially better would be to actually push the YAML to some pipeline path and load it back from executor to avoid having too much data in the entrypoint!
        repo_yaml = None
        with tempfile.TemporaryDirectory() as t:
            repo_config.write_to_path(pathlib.Path(t))
            with open(os.path.join(t, "feature_store.yaml")) as f:
                repo_yaml = f.read()

        custom_config = Struct(
            **{
                executor._REPO_CONFIG: repo_yaml,
                executor._FEATURE_REFS_KEY: feature_refs,
                executor._FEATURE_VIEW_REF_KEY: feature_view_ref,
                executor._COMPUTE_STATS: compute_stats,
            }
        )

        custom_config_pbs2 = example_gen_pb2.CustomConfig()
        custom_config_pbs2.Pack(custom_config)

        super().__init__(
            input_config=input_config, custom_config=custom_config, **kwargs
        )
