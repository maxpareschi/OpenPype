"""Host API required Work Files tool"""
import os
import nuke
from pathlib import Path
from datetime import datetime

from openpype.lib import Logger
from .autosave import get_autosave_files


logger = Logger.get_logger(__name__)


def is_headless():
    """
    Returns:
        bool: headless
    """
    from qtpy import QtWidgets
    return QtWidgets.QApplication.instance() is None

def file_extensions():
    return [".nk"]


def has_unsaved_changes():
    return nuke.root().modified()


def save_file(filepath):
    path = filepath.replace("\\", "/")
    nuke.scriptSaveAs(path)
    nuke.Root()["name"].setValue(path)
    nuke.Root()["project_directory"].setValue(os.path.dirname(path))
    nuke.Root().setModified(False)


def return_autosave_candidate(filepath):
    if is_headless():
        return

    autosaves = get_autosave_files(filepath)
    logger.info(f"OpenPype found these autosaves: {autosaves}")
    if len(autosaves) > 0:
        autosave = autosaves[-1]
        if (Path(autosave) != Path(filepath)
            and Path(autosave).stat().st_mtime>Path(filepath).stat().st_mtime):

            # autosave = nuke.toNode("preferences")["AutoSaveName"].evaluate()
            msg = f"Would you like to load the autosave file?\n{autosave}"
            msg += f"\n{datetime.fromtimestamp(Path(autosave).stat().st_mtime)}"
            if os.path.isfile(autosave) and nuke.ask(msg):
                logger.info(f"Restoring autosave file: {filepath}")
                return autosave


def open_file(filepath):
    filepath = filepath.replace("\\", "/")

    # To remain in the same window, we have to clear the script and read
    # in the contents of the workfile.
    logger.info(f"Opening file with OpenPype {filepath}")
    nuke.scriptClear()
    autosave = return_autosave_candidate(filepath)

    file_to_open = autosave or filepath


    nuke.scriptReadFile(file_to_open)
    nuke.Root()["name"].setValue(filepath)
    nuke.Root()["project_directory"].setValue(os.path.dirname(filepath))
    nuke.Root().setModified(False)
    return True


def current_file():
    current_file = nuke.root().name()

    # Unsaved current file
    if current_file == 'Root':
        return None

    return os.path.normpath(current_file).replace("\\", "/")


def work_root(session):

    work_dir = session["AVALON_WORKDIR"]
    scene_dir = session.get("AVALON_SCENEDIR")
    if scene_dir:
        path = os.path.join(work_dir, scene_dir)
    else:
        path = work_dir

    return os.path.normpath(path).replace("\\", "/")
