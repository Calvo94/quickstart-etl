import os
import json
from dagster import  Definitions

from dagster_poc.assets.assets import pipeline_stage_to_dagster_asset

ROOT_DIR = os.path.dirname(__file__)

STAGES_PATH = os.path.join(ROOT_DIR,"stages.json")
STAGES_DICT = json.load(open(STAGES_PATH))
ASSETS = [
    pipeline_stage_to_dagster_asset(
        stage_id=stage_id,
        input_datasets=datasets["input_datasets"],
        output_datasets=datasets["output_datasets"],
    )
    for stage_id, datasets in STAGES_DICT.items()
]



dagster_poc = Definitions(
    assets=ASSETS

)
