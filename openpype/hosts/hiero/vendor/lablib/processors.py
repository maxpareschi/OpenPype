from __future__ import annotations

from typing import Any
from dataclasses import dataclass, field
from logging import Logger, getLogger

import json
import inspect
import os
import uuid
import copy

import PyOpenColorIO as OCIO

from . import operators as ops


@dataclass
class EffectsFileProcessor:
    src: str

    @property
    def color_operators(self) -> dict:
        return self._color_ops
    
    @color_operators.setter
    def color_operators(self, color_ops: list) -> None:
        self._color_ops = color_ops

    @color_operators.deleter
    def color_operators(self) -> None:
        self._color_ops = []
    
    @property
    def repo_operators(self) -> dict:
        return self._repo_ops
    
    @repo_operators.setter
    def repo_operators(self, repo_ops: list) -> None:
        self._repo_ops = repo_ops

    @repo_operators.deleter
    def repo_operators(self) -> None:
        self._repo_ops = []

    def __post_init__(self) -> None:
        self._wrapper_class_members = dict(inspect.getmembers(ops, inspect.isclass))
        self._wrapper_class_names = [v for v in self._wrapper_class_members.keys()]
        self._color_ops: list = list()
        self._repo_ops: list = list()
        self._class_search_key: str = "class"
        self._index_search_key: str = "subTrackIndex"
        self._data_search_key: str = "node"
        self._valid_attrs: tuple = tuple((
            "in_colorspace",
            "out_colorspace",
            "file",
            "saturation",
            "display",
            "view",
            "translate",
            "rotate",
            "scale",
            "center",
            "power",
            "offset",
            "slope",
            "direction"
        ))
        self._valid_attrs_mapping: dict = dict({
            "in_colorspace": "src",
            "out_colorspace": "dst",
            "file": "src",
            "saturation": "sat"
        })
        if self.src:
            self.load(self.src)

    def _get_operator_class(self, name: str) -> Any:
        name = "{}Transform".format(name.replace("OCIO", "").replace("Transform", ""))
        if name in self._wrapper_class_names:
            return self._wrapper_class_members[name]
        elif "Repo{}".format(name) in self._wrapper_class_names:
            return self._wrapper_class_members["Repo{}".format(name)]
        else:
            return None

    def _get_operator_sanitized(self, op: Any, data: dict) -> Any:
        # sanitize for different source data structures.
        # fix for nuke vs ocio, cdl transform should not have a src field by ocio specs
        if "CDL" in op.__name__:
            del data["src"]
        return op(**data)

    def _get_operator(self, data: dict) -> None:
        result = {}
        for k, v in data[self._data_search_key].items():
            if k in self._valid_attrs:
                if k in self._valid_attrs_mapping:
                    result[self._valid_attrs_mapping[k]] = v
                else:
                    if k == "scale" and isinstance(v, float):
                        v = [v, v]
                    result[k] = v
        op = self._get_operator_class(data[self._class_search_key])
        return self._get_operator_sanitized(op = op, data = result)

    def _load(self) -> None:
        with open(self.src, "r") as f:
            _ops = json.load(f)
        ocio_nodes = []
        repo_nodes = []
        for k, v in _ops.items():
            if isinstance(v, dict):
                class_name = "{}Transform".format(
                    v[self._class_search_key].replace("OCIO", "").replace("Transform", "")
                )
                if class_name in self._wrapper_class_names:
                    ocio_nodes.append(v)
                elif "Repo{}".format(class_name) in self._wrapper_class_names:
                    repo_nodes.append(v)
                else:
                    continue
        ocio_nodes = sorted(ocio_nodes, key=lambda d: d[self._index_search_key])
        repo_nodes = sorted(repo_nodes, key=lambda d: d[self._index_search_key])
        for c in ocio_nodes:
            self._color_ops.append(self._get_operator(c))
        for c in repo_nodes:
            self._repo_ops.append(self._get_operator(c))

    def clear_operators(self) -> None:
        self.color_ops = []
        self.repo_ops = []

    def load(self, src: str) -> None:
        self.src = src
        self.clear_operators()
        self._load()


@dataclass
class ColorProcessor:
    operators: list = field(default_factory = list)
    config_path: str = None
    staging_dir: str = None
    context: str = "LabLib"
    family: str = "LabLib"
    working_space: str = "ACES - ACEScg"
    views: list[str] = field(default_factory = list)
    log: Logger = field(default_factory = lambda: getLogger("<__lablib__>"))

    def __post_init__(self) -> None:
        if not self.config_path:
            self.config_path = os.path.abspath(os.environ.get("OCIO")).replace("\\", "/")
        if not self.staging_dir:
            self.staging_dir = os.path.abspath(
                os.path.join(
                    os.environ.get("TEMP", os.environ["TMP"]),
                    "LabLib",
                    str(uuid.uuid4())
                )
            ).replace("\\", "/")
        self._description: str = None
        self._vars: dict = dict()
        self._views: list[str] = list()
        if self.views:
            self.set_views(self.views)       
        self._ocio_config: OCIO.Config = None
        self._ocio_transforms: list = list()
        self._ocio_search_paths: list = list()
        self._ocio_config_name: str = "config.ocio"
        self._dest_path: str = None

    def set_ocio_config_name(self, name: str) -> None:
        self._ocio_config_name = name

    def set_staging_dir(self, path: str) -> None:
        self.staging_dir = os.path.abspath(path).replace("\\", "/")
    
    def set_views(self, *args) -> None:
        self.clear_views()
        self.append_views(*args)

    def set_operators(self, *args) -> None:
        self.clear_operators()
        self.append_operators(*args)

    def set_vars(self, **kwargs) -> None:
        self.clear_vars()
        self.append_vars(**kwargs)
    
    def set_description(self, desc: str) -> None:
        self._description = desc

    def clear_operators(self) -> None:
        self.operators = list()

    def clear_views(self):
        self._views = list()

    def clear_vars(self):
        self._vars = dict()

    def append_operators(self, *args) -> None:
        for i in args:
            if isinstance(i, list) or isinstance(i, tuple):
                self.append_operators(*i)
            else:
                self.operators.append(i)

    def append_views(self, *args) -> None:            
        for i in args:
            if isinstance(i, list) or isinstance(i, tuple):
                self.append_views(*i)
            else:
                self._views.append(i)

    def append_vars(self, **kwargs) -> None:
        for k, v in kwargs.items():
            self._vars[k] = v
    
    def get_config_path(self) -> str:
        return self._dest_path

    def get_description_from_config(self) -> str:
        return self._ocio_config.getDescription()
    
    def _get_search_paths_from_config(self) -> list:
        return list(self._ocio_config.getSearchPaths())
    
    def _sanitize_search_paths(self, paths: list) -> list:
        real_paths = []
        for p in paths:
            computed_path = os.path.abspath(
                os.path.join(os.path.dirname(self.config_path), p)
            ).replace("\\", "/")
            if os.path.isfile(computed_path):
                computed_path = os.path.abspath(
                    os.path.dirname(computed_path)
                ).replace("\\", "/")
                real_paths.append(computed_path)
            elif os.path.isdir(computed_path):
                real_paths.append(computed_path)
            else:
                continue
        real_paths = list(set(real_paths))
        self._search_paths = real_paths
        return real_paths
    
    def _get_absolute_search_paths_from_ocio(self) -> list:
        paths = self._get_search_paths_from_config()
        for op in self._ocio_transforms:
            try:
                paths.append(op.getSrc())
            except:
                continue
        return self._sanitize_search_paths(paths)
    
    def _get_absolute_search_paths(self) -> list:
        paths = self._get_search_paths_from_config()
        for op in self.operators:
            if hasattr(op, "src"):
                paths.append(op.src)
        return self._sanitize_search_paths(paths)

    def _read_config(self) -> None:
        self._ocio_config = OCIO.Config.CreateFromFile(self.config_path)

    def load_config_from_file(self, src: str) -> None:
        self.config_path = src
        self._read_config()

    def process_config(self) -> None:
        for op in self.operators:
            self.log.debug(f"Processing operator: '{op}'")
            props = vars(op)
            if props.get("direction"):
                props["direction"] = OCIO.TransformDirection.TRANSFORM_DIR_INVERSE
            else:
                props["direction"] = OCIO.TransformDirection.TRANSFORM_DIR_FORWARD
            ocio_class_name = getattr(OCIO, op.__class__.__name__)
            if props.get("src"):
                op_path = os.path.abspath(props["src"]).replace("\\", "/")
                if os.path.isfile(op_path):
                    props["src"] = op_path.name
            self.log.debug(f"Operator '{ocio_class_name}' parameters: '{props}'")
            self._ocio_transforms.append(ocio_class_name(**props))
        for k, v in self._vars.items():
            self._ocio_config.addEnvironmentVar(k, v)
            self.log.debug(f"Added env var '{k}': '{v}'")
        self._ocio_config.setDescription(self._description)
        group_transform = OCIO.GroupTransform(self._ocio_transforms)
        look_transform = OCIO.ColorSpaceTransform(
            src = self.working_space,
            dst = self.context
        )
        cspace = OCIO.ColorSpace()
        cspace.setName(self.context)
        cspace.setFamily(self.family)
        cspace.setTransform(
            group_transform,
            OCIO.ColorSpaceDirection.COLORSPACE_DIR_FROM_REFERENCE
        )
        look = OCIO.Look(
            name = self.context,
            processSpace = self.working_space,
            transform = look_transform
        )
        self._ocio_config.addColorSpace(cspace)
        self.log.debug(f"Added colorspace '{cspace}'")
        self._ocio_config.addLook(look)
        self.log.debug(f"Added look '{look}'")
        self._ocio_config.addDisplayView(
            self._ocio_config.getActiveDisplays().split(",")[0],
            self.context,
            self.working_space,
            looks = self.context
        )
        if not self._views:
            active_views = self._ocio_config.getActiveViews()
        else:
            active_views = ",".join(self._views)
        
        self.log.debug(f"Current active views: '{active_views}'")
        self._ocio_config.setActiveViews(
            "{},{}".format(self.context, active_views))

        self._ocio_config.validate()
        self.log.debug(f"Added active views: '{self._ocio_config.getActiveViews()}'")

    def write_config(self, dest: str = None) -> str:
        config_lines = self._ocio_config.serialize().splitlines()
        search_paths = self._search_paths
        for i, sp in enumerate(search_paths):
            search_paths[i] = "  - {}".format(sp)
        for i, l in enumerate(copy.deepcopy(config_lines)):
            if l.find("search_path") >= 0:
                config_lines[i] = "\nsearch_path:"
                for idx, sp in enumerate(search_paths):
                    config_lines.insert(i+idx+1, sp)
                config_lines.insert(i+len(search_paths)+1, "")
                break
        final_config = "\n".join(config_lines)
        dest = os.path.abspath(dest).replace("\\", "/")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "w") as f:
            f.write(final_config)
        return final_config

    def create_config(self, dest: str = None) -> None:
        if not dest: dest = os.path.abspath(
            os.path.join(self.staging_dir, self._ocio_config_name)
        ).replace("\\", "/")
        dest = os.path.abspath(dest).replace("\\", "/")
        self.load_config_from_file(os.path.abspath(self.config_path).replace("\\", "/"))
        self._get_absolute_search_paths()
        self.process_config()
        self.write_config(dest)
        self._dest_path = dest
        return dest
    
    def get_oiiotool_cmd(self) -> list:
        cmd = [
            "--colorconfig",
            self._dest_path,
            "--ociolook:from=\"{}\":to=\"{}\"".format(self.working_space,
                                                      self.working_space),
            self.context
        ]
        return cmd

