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
from tfx.components.example_gen import component
from tfx.dsl.components.base import executor_spec
from tfx.proto import example_gen_pb2

from . import executor


class FeastExampleGen(component.QueryBasedExampleGen):

    EXECUTOR_SPEC = executor_spec.BeamExecutorSpec(executor.Executor)

    def __init__(
        self,
        repo_config: feast.RepoConfig,
        features: Union[List[str], str, feast.FeatureService],
        entity_query: Optional[str] = None,
        **kwargs
    ):
        """FeastExampleGen is a way for loading offline features. For now we support BQ.

        Example:

            example_gen = FeastExampleGen(
                repo_config=RepoConfig(registr="gs://..."),
                entity_query="SELECT user, timestamp from some_user_dataset",
                features=["f1", "f2"],
            )

        Args:
            repo_config (feast.RepoConfig): Feast repo configuration object
            features (Union[List[str] str, feast.FeatureService]): List of features to join on the dataset or feaure service identifier.
            entity_query (Optional[str], optional): Query used to obtain the entity dataframe. Defaults to None.
            **kwargs: kwargs used in QueryBasedExampleGen
        """
        # Serialize repo config into a YAML to pass it to the executor
        # ToDo: Potentially better would be to actually push the YAML to some pipeline path and load it back from executor to avoid having too much data in the entrypoint!
        repo_yaml = None
        with tempfile.TemporaryDirectory() as t:
            repo_config.write_to_path(pathlib.Path(t))
            with open(os.path.join(t, "feature_store.yaml")) as f:
                repo_yaml = f.read()
        custom_config = Struct()
        custom_config.update(
            {
                executor._REPO_CONFIG_KEY: repo_yaml,
            }
        )
        if isinstance(features, str):
            custom_config.update(
                {
                    executor._FEATURE_SERVICE_KEY: features,
                }
            )
        elif isinstance(features, list):
            custom_config.update(
                {
                    executor._FEATURE_KEY: features,
                }
            )
        elif isinstance(features, feast.FeatureService):
            custom_config.update(
                {
                    executor._FEATURE_SERVICE_KEY: features.name,
                }
            )
        else:
            raise RuntimeError(
                "Features argument not recognized."
                " Found {type(features)} but expected feature service or list of features"
            )

        # Store configuration as part of a protobuf struct and pack inside custom_config
        custom_config_pbs2 = example_gen_pb2.CustomConfig()
        custom_config_pbs2.Pack(custom_config)

        super().__init__(custom_config=custom_config, query=entity_query, **kwargs)
