from typing import Dict, Tuple,List
import re
import logging

from dagster import (
    multi_asset,
    AssetOut,
    AssetKey,
    AssetsDefinition,
    OpExecutionContext,
)



def pipeline_stage_to_dagster_asset(
    stage_id: str,
    input_datasets: List[str],
    output_datasets: List[str],
) -> AssetsDefinition:

    # outputs
    outputs = dict()
    output_dataset_ids = set()
    for dataset in output_datasets:
        asset_name, output = asset_out_from_dataset(dataset)
        outputs[asset_name] = output
        output_dataset_ids.add(dataset)

    # inputs
    inputs = set()
    for dataset in input_datasets:
        # PIMMS dataset creates a circular dependency in our pipeline
        if (
            dataset == "pimms_stacked_company"
            and stage_id == "beacon__create_identity_id_stacked_output"
        ):
            continue
        # same, ignore self-dependency to avoid a circular dependency
        if dataset in output_dataset_ids:
            continue

        inputs.add(asset_key_from_dataset(dataset))

    # optional
    stage_group = get_stage_group_name(stage_id)

    @multi_asset(
        non_argument_deps=inputs,
        outs=outputs,
        name=f'{stage_id.replace(".", "__")}',
    )
    def inner(context: OpExecutionContext):
        logging.info(f"Run {stage_id}")

    return inner


#####
def asset_out_from_dataset(dataset) -> Tuple[str, AssetOut]:
    """
    Constructs and AssetOut object from the given dataset
    """
    asset_name = dataset.replace(".", "__")

    return asset_name, AssetOut(
        key=asset_name,
        
    )


def asset_key_from_dataset(dataset) -> AssetKey:
    """
    Constructs and AssetKey object from the given dataset
    """
    return AssetKey(dataset.split(".").pop())


# group name --> stage id pattern
GROUPS = {
    "expectations": r"^expectations__.*$",
    "digestors": r"^digestors__.*$",
    "cleaning": r"^cleaners__.*$",
    "entity_extraction": r"^entity_extracters__.*$",
    "standardization": r"^standardizers__.*$",
    "stacking": r"^stacking\..*$",
    "entity_resolution": r"^(?!beacon__denorm)(resolution|beacon)__.*$",
    "denorm": r"^beacon__denorm$",
    "sourdough": r"^sourdough__.*$",
    "supplementors": r"^supplementors__.*$",
}


def get_stage_group_name(stage, groups: Dict[str, str] = GROUPS) -> str:
    """
    Get the group name to which the given stage belongs to
    """
    for group_name, pattern in groups.items():
        if re.match(pattern, stage):
            return group_name

    return None
