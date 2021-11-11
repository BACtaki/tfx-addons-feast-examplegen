from tfx.types.component_spec import ComponentSpec

from tfx.types import standard_component_specs

from tfx.types import standard_artifacts
from tfx.types.component_spec import ChannelParameter


class QueryBasedExampleGenWithStatsSpec(
    standard_component_specs.QueryBasedExampleGenSpec
):
    """Query-based ExampleGen component with optional statistics output spec."""

    INPUTS = {}
    OUTPUTS = {
        standard_component_specs.STATISTICS_KEY: ChannelParameter(
            type=standard_artifacts.ExampleStatistics, optional=True
        ),
        **standard_component_specs.QueryBasedExampleGenSpec.OUTPUTS,
    }
