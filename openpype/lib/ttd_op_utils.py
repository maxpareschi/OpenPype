from typing import List, Union, Dict
from pathlib import Path
from logging import getLogger
from copy import deepcopy
import webbrowser
from subprocess import Popen, PIPE
from datetime import datetime
from tempfile import NamedTemporaryFile
from collections import defaultdict
from re import compile  as recomp, Pattern

from ftrack_api.entity.asset_version import AssetVersion
from openpype.settings import get_anatomy_settings, get_project_settings


logger = getLogger(__name__)

from openpype.client import get_representations_parents
from openpype.lib import StringTemplate, get_datetime_data
from openpype.pipeline import Anatomy
from openpype.settings import get_project_settings
from openpype.lib import get_oiio_tools_path, get_ffmpeg_tool_path


EMPTY_IMAGE_REGEX = recomp("Stats StdDev: 0.00 0.00 0.00")
INTENT_REGEX = "(WIP\ [A-Z]+)|PAF(?=\ \-\ )"


def augment_representation_context(prj: str, repre: dict, context: dict):
    """Augment repre's context with data from mongoDB about their parents.

    This is done so that producers can access via OpenPype settings to add in
    extradata in a CSV related to a given delivery version.
    """
    asset = get_representations_parents(prj, [repre])[repre["_id"]][0]
    start = asset["data"]["frameStart"] - asset["data"]["handleStart"]
    end = asset["data"]["frameEnd"] + asset["data"]["handleEnd"]
    context["asset_data"] = deepcopy(asset["data"])
    context["asset_data"]["duration"] = end - start + 2
    context["asset_data"]["duration_no_slate"] = end - start + 1
    context["asset_data"]["start"] = start
    context["asset_data"]["end"] = end


def escape_commas_in_csv_cell(cell: str):
    if "," in cell:
        if "\"" in cell:
            cell = cell.replace("\"", "\"\"") # escape double quotes
        cell = f"\"{cell}\"" # surrond with double quotes to escape commas
    return cell


def generate_csv_line_from_repre(
    prj: str, repre: dict, anatomy: Anatomy, datetime_data: dict, settings: dict
):
    """Generates a single CSV line for a given delivery representaion."""

    data = deepcopy(repre["context"])
    data["root"] = anatomy.roots
    data.update(datetime_data)
    # template = ",".join([d["column_value"] for d in settings])
    augment_representation_context(prj, repre, data)
    row = ""
    for item in settings:
        t = item["column_value"]
        try:
            value = StringTemplate.format_strict_template(t, data)
        except:
            value = ""
        value = escape_commas_in_csv_cell(value)
        row = row + value + ","
    return row[:-1]


def yield_csv_lines_from_repres(
    prj: str, representations: List[dict], anatomy_name: str
):
    """Yield one CSV line per representation passed in the format of anatomy_name."""

    anatomy = Anatomy(prj)
    datetime_data = get_datetime_data()
    settings = get_project_settings(prj)["ftrack"]["user_handlers"]
    tpl = settings["delivery_action"]["csv_template_families"].get(anatomy_name)
    if tpl is None:
        tpl = settings["delivery_action"]["csv_template_families"].get("default")
    if tpl is None:
        tpl = [
            {"column_name": "Errors", "column_value": "Failed to find CSV config"}
        ]
    yield ",".join([d["column_name"] for d in tpl])
    for repre in representations:
        yield generate_csv_line_from_repre(prj, repre, anatomy, datetime_data, tpl)


def generate_csv_from_representations(
    prj: str, repres: List[dict], csv_path: Union[Path, str], anatomy_name: str
):
    """Create a CSV for the given representations in the given path."""

    csv = Path(csv_path)
    logger.info(f"Generating csv file {csv}, {csv.parent}")
    csv.parent.mkdir(exist_ok=True, parents=True)
    lines = "\n".join(yield_csv_lines_from_repres(prj, repres, anatomy_name))
    csv.write_text(lines)
    webbrowser.open(csv)


def by_alphabet(representation: dict):
    """Utility function used in `sort` which orders representations alphabetically"""

    return representation["files"][0]["path"].split("/")[-1]


def by_version(representation: dict):
    """Utility function used in `sort` which orders representations by version"""

    return representation["context"]["version"]


def return_version_notes_for_csv(version: AssetVersion):
    """Utility function used to inject the Ftrack ASsetVersions notes into a CSV.

    It iterates the AssetVersion notes and returns a dictionary with the notes
    classified by note tags. Several notes for the same note tag are separated by dots.
    {
        "to_client": "My note one. My note two",
        "from_client": "Very good. Approved. Love it."
    }

    """

    result: Dict[str,str] = defaultdict(str)
    for note in version["notes"]:
        if note["category"] is None:
            continue
        if note["in_reply_to"]:
            continue
        content = note["content"]

        if note["replies"]:
            replies = ". ".join([n["content"] for n in note["replies"]])
            content = content + ". " + replies
        result[note["category"]["name"]] += content

    # so that they can do both For Client and FOR CLIENT...
    for k, v in deepcopy(result).items():
        if k.upper() not in result.keys():
            result[k.upper()] = v

    return result

def return_intent_from_notes(notes: Dict[str,str], intent_regexes: List[str]):
    result = defaultdict(str)
    intent_patterns = [recomp(i) for i in intent_regexes]
    for k, v in notes.items():
        for regex in intent_patterns:
            match = regex.match(v)
            if match:
                result[k] = match.group()
                break
    return result


def extract_channel(ffmpeg_bin: str, source: str, channel: str = "a"):
    """Use `ffmpeg_bin` to save channel `channel` from `source` into a tempfile.

    `channel` must be a letter in [y, u, v, r, g, b, a]
    """

    assert channel in "yuvrgba", f"channel must be in {list('yuvrgba')}"
    assert Path(source).exists()

    tempfile = NamedTemporaryFile(suffix=".jpg", delete=False)
    cmd = [
        ffmpeg_bin,
        "-y",
        "-i",
        source,
        "-filter_complex",
        f"extractplanes={channel}",
        tempfile.name,
    ]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, bufsize=10**8)
    output, error = p.communicate()
    logger.info(f"{output.decode()}\n{error.decode()}")
    logger.info(f"Extracted channel {channel} into file {tempfile.name}")
    return tempfile.name


def get_color_data_from_image(iinfo_bin: str, source: str):
    """Return color stats from `source` using `iinfo_bin`."""

    cmd = [iinfo_bin, "--stats", source]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, bufsize=10**8)
    output, error = p.communicate()
    return f"{output.decode()}\n{error.decode()}"


def is_image_empty(iinfo_bin: str, file: str):
    """Use `iinfo_bin` to retunr True if `file` is completelly white, else False."""

    out = get_color_data_from_image(iinfo_bin, file)
    return bool(EMPTY_IMAGE_REGEX.findall(out))

def is_alpha_channel_empty(image: str):
    """Return True if `image` alpha channel has info, else False.
    
    This is used to check whether an EXR has matte
    """
    iinfo = get_oiio_tools_path("iinfo")
    ffmpeg = get_ffmpeg_tool_path("ffmpeg")
    return is_image_empty(iinfo, extract_channel(ffmpeg, image, "a"))


def augment_repre_with_ftrack_version_data(
    repre: dict, version: AssetVersion, submission_name: str = ""
):
    """Utility function that adds extra Ftrack data to its OP representation.

    This function was created for the CSV action, so that any data requested by
    the producers to be in the CSV, can be there for them to customize the CSV,
    nonetheless this function can be used elsewhere.

    Currently the data added from the Ftrack AssetVersion into the repre context is:
        * `version_info`: dict containing `name`, `version` and `fullname`
        * `episode_name`
        * `sequence_name`
        * `shot_name`
        * `status`
        * `notes`
    """
    prj = version["project"]["full_name"]
    templates = get_anatomy_settings(prj)["templates"]
    padding = int(templates["defaults"]["version_padding"])
    # padding = 3
    name = version["asset"]["name"]
    ctx = repre["context"]
    ctx["submission_name"] = submission_name
    ctx["version_info"] = {
        "name": name,
        "version": version["version"],
        "padded_version": "v" + str(version["version"]).zfill(padding),
        "full_name": name + "_v" + str(version["version"]).zfill(padding),
    }

    if len(version["incoming_links"]) > 0:
        internal_working_version = version["incoming_links"][0]["from"]
    else:
        internal_working_version = version

    ctx["episode_name"] = return_episode_from_version(internal_working_version)["name"]
    ctx["sequence_name"] = return_sequence_from_version(internal_working_version)[
        "name"
    ]
    shot = return_shot_from_version(internal_working_version)
    ctx["shot_name"] = shot["name"]

    file = repre["files"][1] if len(repre["files"]) > 1 else repre["files"][0]
    data = deepcopy(repre["context"])
    data["root"] = Anatomy(version["project"]["full_name"]).roots

    file = StringTemplate.format_strict_template(file["path"], data)

    # ctx["exr_includes_matte"] = "" if is_alpha_channel_empty(file) else "X"
    ctx["exr_includes_matte"] = ""
    ctx["status"] = version["status"]["name"]
    ctx["notes"] = return_version_notes_for_csv(version)
    prj_settings = get_project_settings(prj)["ftrack"]["user_handlers"]
    try:
        intent_regexes = prj_settings["delivery_action"]["csv_intent_regex"]
    except KeyError as e:
        intent_regexes = [
                "(WIP\\ [A-Z]+)|PAF(?=\\ \\-\\ )",
                "(WIP(?=\\ \\-\\ ))",
                "(FINAL\\ PENDING\\ TECH\\ CHECK(?=\\ \\-\\ ))",
                "(FINAL\\ TECH\\ CHECKED(?=\\ \\-\\ ))",
                "(APROVAL(?=\\ \\-\\ ))",
                "(REFERENCE(?=\\ \\-\\ ))",
                "(TEMP(?=\\ \\-\\ ))",
                "(MARKETING(?=\\ \\-\\ ))",
                "(TEST(?=\\ \\-\\ ))"
            ]
    ctx["intent"] = return_intent_from_notes(ctx["notes"], intent_regexes)


def get_csv_path(created_files: List[str], pckg_name: str):
    """Utility function that returns the path to the CSV of a delivery.

    If the delivery doesn't come from a list, pckg_name may be falsy and
    no path would be returned, so that a temp csv can be used instead."""

    if pckg_name and pckg_name in created_files[0]:
        return (
            created_files[0].split(pckg_name)[0] + f"/{pckg_name}/{pckg_name}.csv"
        )


def create_csv_in_download_folder(
    prj: str, name: str, repres: List[dict], anatomy_name: str
):
    """Creates a CSV in the download folder given list of repres and a delivery profile.

    After the CSV file is created, it opens the file so that the user has feedback
    and can save it to another directory.
    """
    csv_lines = "\n".join(yield_csv_lines_from_repres(prj, repres, anatomy_name))
    filename = datetime.now().strftime("%Y%m%d_%H_%M") + "_delivery_data"
    dl_dir = Path.home() / f"Downloads/{name or filename}.csv"
    dl_dir.write_text(csv_lines)
    webbrowser.open(dl_dir)


def create_temp_csv(prj: str, name: str, repres: List[dict], anatomy_name: str):
    """Creates a CSV file in a tempdir from a list of repres and a delivery profile.

    After the CSV file is created, it opens the file so that the user has feedback
    and can save it to another directory.
    """

    lines = "\n".join(yield_csv_lines_from_repres(prj, repres, anatomy_name))
    with NamedTemporaryFile(mode="w", delete=False, prefix=name, suffix=".csv") as fp:
        fp.write(lines)
    webbrowser.open(fp.name)


def fetch_ancesor_of_type(ancestors: list, type_: str):
    """Utility function that returns a matching ancestor for a given type."""
    return next((e for e in ancestors if e.entity_type == type_), None)


def return_ancestor_of_type_from_version(version: AssetVersion, type_: str):
    """Looks for an ancestor of a given type in an Ftrack AssetVersion.

    This is used to climb up the hierarchy and figure out the parents of an
    AssetVersion such as Shot, Episode, Asset, Sequence or whatever.
    """

    ancestor = fetch_ancesor_of_type(list(version["asset"]["ancestors"]), type_)
    if ancestor:
        return ancestor
    ancestor = fetch_ancesor_of_type(list(version["asset"]["parent"]["ancestors"]), type_)
    if ancestor:
        return ancestor
    else:
        logger.warning(f"Failed to find {type_} for version {version}.")
    return {"name": ""}


def return_sequence_from_version(version: AssetVersion):
    """Attempts to return the sequence an AssetVersion belongs to."""

    return return_ancestor_of_type_from_version(version, "Sequence")


def return_episode_from_version(version: AssetVersion):
    """Attempts to return the episode an AssetVersion belongs to."""

    return return_ancestor_of_type_from_version(version, "Episode")


def return_asset_from_version(version: AssetVersion):
    """Attempts to return the assetbuild an AssetVersion belongs to."""

    return return_ancestor_of_type_from_version(version, "AssetBuild")


def return_shot_from_version(version: AssetVersion):
    """Attempts to return the shot an AssetVersion belongs to."""

    return return_ancestor_of_type_from_version(version, "Shot")


def handle_csv(
    report: dict, name: str, prj: str, repres: List[dict], cfg: str, order: str
):

    repres = sorted(repres, key=by_alphabet)
    if order == "version":
        repres = sorted(repres, key=by_version)

    try:
        csv_file = get_csv_path(report["created_files"], name)
    except IndexError as e:
        raise NotImplementedError(report) from e

    if csv_file is not None:
        generate_csv_from_representations(prj, repres, csv_file, cfg)
        logger.info(f"CSV saved in {csv_file}")
    else:
        try:
            create_csv_in_download_folder(prj, name, repres, cfg)
        except:
            # in case the download folder fails to be found
            create_temp_csv(prj, name, repres, cfg)
