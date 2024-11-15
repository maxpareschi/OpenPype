import os
import json

if __name__ == "__main__":
    current_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "plugins"
    )
    plugin_paths = {}
    for dir in os.scandir(current_dir):
        if dir.is_dir():
            key = os.path.basename(dir)
            plugin_paths.update(
                {
                    key: [
                        os.path.join(current_dir, key).replace("\\", "/")
                    ]
                }
            )
            for root, dirs, files in os.walk(dir, topdown=False):
                for d in dirs:
                    plugin_paths[key].append(os.path.normpath(os.path.join(root, d)).replace("\\", "/"))
    
    print(json.dumps(plugin_paths, indent=4, default=str))