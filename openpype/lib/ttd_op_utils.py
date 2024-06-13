from typing import List, Union
from pathlib import Path
from logging import getLogger
from copy import deepcopy

logger = getLogger(__name__)

from openpype.client import get_representations_parents
from openpype.lib import StringTemplate, get_datetime_data
from openpype.pipeline import Anatomy
from openpype.settings import get_project_settings

def augment_representation_context(prj: str, repre: dict, context: dict):
    asset = get_representations_parents(prj, [repre])[repre["_id"]][0]
    start = asset["data"]["frameStart"] - asset["data"]["handleStart"]
    end = asset["data"]["frameEnd"] + asset["data"]["handleEnd"]
    context["asset_data"] = deepcopy(asset["data"])
    context["asset_data"]["duration"] = end - start
    context["asset_data"]["start"] = start
    context["asset_data"]["end"] = end

def generate_csv_line_from_repre(
        prj: str, repre: dict, anatomy: Anatomy, datetime_data: dict, settings: dict
    ):
    data = deepcopy(repre["context"])
    data["root"] = anatomy.roots
    data.update(datetime_data)
    # template = ",".join([d["column_value"] for d in settings])
    augment_representation_context(prj, repre, data)
    row =  ""
    for item in settings:
        t = item["column_value"]
        try:
            value = StringTemplate.format_strict_template(t, data)
        except:
            value = ""
        row = row + value + ","
    return row[:-1]



def yield_csv_lines_from_representations(
        prj: str, representations: List[dict], anatomy_name: str):
    anatomy = Anatomy(prj)
    datetime_data = get_datetime_data()
    settings = get_project_settings(prj)["ftrack"]["user_handlers"]
    settings = settings["delivery_action"]["csv_template_families"].get(anatomy_name)
    if settings is None:
        settings = [{"column_name":"Errors", "column_value":"Failed to find CSV config"}]
    yield ",".join([d["column_name"] for d in settings])
    for repre in representations:
        yield generate_csv_line_from_repre(prj, repre, anatomy, datetime_data, settings)


def generate_csv_from_representations(
        prj: str, representations: List[dict], csv_path: Union[Path, str], anatomy_name: str):
    csv = Path(csv_path)
    print(f"Generating csv file {csv}, {csv.parent}")
    csv.parent.mkdir(exist_ok=True, parents=True)
    lines = "\n".join(yield_csv_lines_from_representations(prj, representations, anatomy_name))
    csv.write_text(lines)