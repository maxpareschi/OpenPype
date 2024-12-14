import os
import tempfile
import datetime
import csv
import logging
import platform

if platform.system().lower() == "windows":
    from ctypes import create_unicode_buffer, windll

import ftrack_api
from aiohttp import web
from openpype.modules.webserver import WebServerModule


def file_match(name, files, threshold = 8):
    matches = []
    for file in files:
        count = 0
        for char1, char2 in zip(name, file):
            if char1 == char2:
                count += 1
            else:
                break
        if count >= threshold:
            matches.append(file)

    return matches

def read_csv(csv_file):

    allowed_exts = [".jpg", ".jpeg", ".png"]
    csv_rows = []
    dirpath = os.path.dirname(csv_file)
    files = os.listdir(dirpath) or []

    with open(csv_file, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            row_dict = dict(row)
            name_splits = row["name"].split("_v")
            row_dict.update({
                "assetversion_name": name_splits[-2],
                "version": int(name_splits[-1]),
                "label": "Client feedback",
                "annotations": []
            })
            csv_rows.append(row_dict)

    for row in csv_rows:
        tagged_files = file_match(row["name"], files)
        for file in tagged_files:
            if os.path.splitext(os.path.basename(file))[1] in allowed_exts:
                row["annotations"].append(os.path.join(dirpath, file).replace("\\", "/"))
    
    return csv_rows

def get_assetversion(session, assetversion_name, version, project_name=None):
    project_query_string = ""
    if project_name:
        project_query_string = f"project.name is {project_name} and "
    return session.query((f"AssetVersion where {project_query_string}"
                          f"version is {version} and"
                          f"asset.name is {assetversion_name}")).first()

def cache_statuses(session, log = None):
    if not log:
        log = logging.getLogger("cache_statuses")
    status_dict = {}
    ftrack_statuses = session.query("Status").all()
    for status in ftrack_statuses:
        status_dict.update({
            status["name"]: status
        })
    log.debug(f"Cached statuses: {status_dict}")
    return status_dict

def cache_note_labels(session, log = None):
    if not log:
        log = logging.getLogger("cache_note_labels")
    labels_dict = {}
    ftrack_labels = session.query("NoteLabel").all()
    for label in ftrack_labels:
        labels_dict.update({
            label["name"]: label
        })
    log.debug(f"Cached NoteLabels: {labels_dict}")
    return labels_dict

def import_csv_notes(session, csv_list, user=None, project_name=None, log = None):

    if not log:
        log = logging.getLogger("import_csv_notes")
    if not user:
        user = session.api_user
    status_cache = cache_statuses(session, log=log)
    label_cache = cache_note_labels(session, log=log)
    server_location = session.query("Location where name is 'ftrack.server'").one()
    
    for csv_data in csv_list:
        assetversion = get_assetversion(
            session=session,
            assetversion_name=csv_data["assetversion_name"],
            version=csv_data["version"],
            project_name=project_name
        )
        log.debug(f"Found AssetVersion: {assetversion}: {assetversion['asset']['name']}")
        assetversion["status"] = status_cache[csv_data["status"]]
        log.debug(f"Changed {assetversion['asset']['name']} status to: {csv_data['status']}")
        note = assetversion.create_note(
            csv_data["note"],
            author=user,
            labels=[label_cache[csv_data["label"]]]
        )
        log.debug(f"Created Note: {note}: {note['content']}")
        for _idx, path in enumerate(csv_data["annotations"]):
            annot_name, annot_ext = os.path.splitext(os.path.basename(path))  
            component = session.create_component(
                path=path,
                data={
                    "name": annot_name + "_annotation_" + str(_idx).zfill(2) + annot_ext
                },
                location=server_location
            )
            log.debug(f"Created Component: {component}: {component['name']}")
            notecomponent = session.create(
                "NoteComponent",
                {
                    "component_id": component["id"],
                    "note_id": note["id"]
                }
            )
            print(f"Created NoteComponent: {notecomponent}")

def sanitize_path(path):
    final_path = path.replace("\\", "/")
    if platform.system().lower() == "windows":
        BUFFER_SIZE = 512
        windows_path = final_path.replace("/", "\\")
        buffer = create_unicode_buffer(BUFFER_SIZE)
        GetLongPathName = windll.kernel32.GetLongPathNameW
        GetLongPathName(windows_path, buffer, BUFFER_SIZE)
        final_path = buffer.value
    return final_path.replace("\\", "/")

class FtrackWebserver:

    def __init__(self,
                 port = None,
                 host = None):
        self.host = host or "localhost"
        self.port = port or 8079
        env_host = os.environ.get("OPENPYPE_FTRACK_WEBSERVER_URL", None)
        if env_host:
            self.host = env_host
        env_port = os.environ.get("OPENPYPE_FTRACK_WEBSERVER_PORT", None)
        if env_port:
            self.port = env_port
        self.session = ftrack_api.Session(auto_connect_event_hub=True)
        self.manager = WebServerModule.create_new_server_manager(self.port, self.host)
        self.routes_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "routes")
        self.log = logging.getLogger("FtrackWebServer")
        self.setup_routes()
        self.cache_pages()
        self.start_server()

    def cache_pages(self):
        with open(os.path.join(self.routes_dir, "upload_csv", "upload_csv.html"), "r") as f:
            self.upload_files_page = f.read()
        
    def setup_routes(self):
        self.manager.add_static("/static", os.path.join(self.routes_dir, "static"))
        self.manager.add_route("GET", "/upload_csv", self.upload_csv)
        self.manager.add_route("POST", "/upload_csv", self.handle_csv_upload)
        self.manager.add_route("GET", "/widget_ready", self.widget_ready)
        self.manager.add_static("/upload_csv", os.path.join(self.routes_dir, "upload_csv"))

    def start_server(self):
        self.manager.start_server()

    async def upload_csv(self, request):
        # with open(os.path.join(self.routes_dir, "upload_csv", "upload_csv.html"), "r") as f:
        #     self.upload_files_page = f.read()
        headers = {
            "ftrack_server": os.environ["FTRACK_SERVER"],
            "ftrack_api_user": os.environ["FTRACK_API_USER"],
            "ftrack_api_key": os.environ["FTRACK_API_KEY"]
        }
        print(f"  - {{ upload_csv }}: [  Returning widget fo ftrack.  ]")
        return web.Response(text=self.upload_files_page,
                            content_type="text/html",
                            headers=headers)
    
    async def widget_ready(self, request):
        widget_ready_event = ftrack_api.event.base.Event(
            topic = "ftrack.widget.ready"
        )
        self.session.event_hub.publish(widget_ready_event)
        return web.json_response({"status": "success"})

    async def handle_csv_upload(self, request):
        temp_dir = tempfile.mkdtemp(prefix="ftrack_webserver_upload_",
                                    suffix=f"_{int(datetime.datetime.now().timestamp())}")        
        files_saved = []
        print(f"  - {{ handle_csv_upload }}: [  Saving uploaded files in: '$TEMP/{os.path.basename(temp_dir)}'  ]")
        reader = await request.multipart()
        async for part in reader:
            if part.filename:
                file_path = os.path.join(temp_dir, part.filename)
                with open(file_path, "wb") as f:
                    while True:
                        chunk = await part.read_chunk()
                        if not chunk:
                            break
                        f.write(chunk)
                files_saved.append(sanitize_path(file_path))
                print(f"  - {{ handle_csv_upload }}: [  Saved '{os.path.basename(file_path)}' to temp dir.  ]")

        csv_list = []
        for file in files_saved:
            if ".csv" in file:
                csv_list.extend(read_csv(file))

        current_user_id = request.rel_url.query["user_id"]
        current_user = self.session.query(f"User where id is '{current_user_id}'").one()
        target = f'applicationId=ftrack.client.web and user.id="{current_user_id}"'

        import_csv_notes(session=self.session,
                         csv_list=csv_list,
                         user=current_user,
                         log=self.log)
        
        try:
            self.session.commit()
        except:
            self.session.rollback()
            self.log.warning("Session was not committed!")
            
        show_banner_event = ftrack_api.event.base.Event(
            topic = "ftrack.action.trigger-user-interface",
            data = {
                "type": "message",
                "success": True,
                "message": "Client CSV Ingest done!"
            },
            target = target
        )
        self.session.event_hub.publish(show_banner_event)

        return web.json_response({"status": "success", "files": files_saved})

