import glob
import time
import os
from pprint import pformat
from openpype.lib import Logger
from pathlib import Path

import nuke

from openpype.settings import get_current_project_settings

logger = Logger.get_logger(__name__)

### Example that implements a rolling autosave using the autoSaveFilter callbacks
###
## autosaves roll from 0-9 eg myfile.autosave, myfile.autosave1, myfile.autosave2...
#
## To use just add 'import nukescripts.autosave' in your init.py
settings = get_current_project_settings()["nuke"]["general"]["autosave"]
increments = settings["increments"]

def on_autosave(filename):

    ## ignore untiled autosave
    if nuke.root().name() == 'Root':
        return filename

    fileNo = 0
    files = get_autosave_files(filename)

    if len(files) > 0 :
        lastFile = files[-1]
        # get the last file number

        if len(lastFile) > 0:
            try:
                fileNo = int(lastFile[-1:])
            except:
                pass

            fileNo = fileNo + 1

    if ( fileNo > increments ):
        fileNo = 0

    # if ( fileNo != 0 ):
    filename = filename + str(fileNo)

    logger.info(f"Autosaving file: {filename}")
    return filename

def on_autosave_restore(filename):
    """To allow to read from not just autosave (default) but also autosave1, 2, 3..."""

    files = get_autosave_files(filename)

    if len(files) > 0:
        filename = files[-1]

    logger.info(f"Restoring autosave file: {filename}")
    return filename

def on_autosave_delete(filename):

    ## only delete untiled autosave
    if nuke.root().name() == 'Root':
        return filename

    # return None here to not delete auto save file
    return None
    
def get_autosave_files(filename):
    date_file_list = []
    # glob(r"X:/prj/OBX/generic/gen/work/render_test/gen_render_test_v006_test*.autosave[0-9]")
    glob_str = filename.replace(".nk", "*") + f'[0-{increments}]'
    logger.info(glob_str)
    files = glob.glob(glob_str)

    for file in files:
        logger.debug(file)
        # retrieves the stats for the current file as a tuple
        # (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        # the tuple element mtime at index 8 is the last-modified-date
        stats = os.stat(file)
        # create tuple (year yyyy, month(1-12), day(1-31), hour(0-23), minute(0-59), second(0-59),
        # weekday(0-6, 0 is monday), Julian day(1-366), daylight flag(-1,0 or 1)) from seconds since epoch
        # note:    this tuple can be sorted properly by date and time
        lastmod_date = time.localtime(stats[8])
        #print image_file, lastmod_date     # test
        # create list of tuples ready for sorting by date
        date_file_tuple = lastmod_date, file
        date_file_list.append(date_file_tuple)
     
    date_file_list.sort()
    autosaves = [ filename for _, filename in date_file_list ]
    logger.info(f"Found following autosaves {pformat(autosaves)}")
    return autosaves


### As an example to remove the callbacks use this code
#nuke.removeAutoSaveFilter( on_autosave )
#nuke.removeAutoSaveRestoreFilter( on_autosave_restore )
#nuke.removeAutoSaveDeleteFilter( on_autosave_delete )

def activate_incremental_autosave():
    try:
            nuke.addAutoSaveFilter( on_autosave )
            # nuke.addAutoSaveRestoreFilter( on_autosave_restore )
            nuke.addAutoSaveDeleteFilter( on_autosave_delete )
    except Exception as e:
            logger.warning(f"Failed to activate incremental autosave: {e}")
    else:
            logger.info(f"Autosave activated successfully")

if settings["enabled"] and settings["increments"] > 0:
    activate_incremental_autosave()
else:
    logger.info(f"Incremental autosave for this project is disabled")