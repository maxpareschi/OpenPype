import pyblish.api


from openpype.pipeline import publish


class ExtractTranscode(publish.Extractor):
    """
        Extracts transcodes from presets filtered by profiles.
        Uses nuke terminal to process, needs a Nuke Render license.
        Ideal for farm or multi output. Works with any video or image
    """

    label = "Extract Transcodes (TTD)"
    order = pyblish.api.ExtractorOrder

    # optional = True

    # Supported extensions
    image_exts = ["exr", "dpx", "jpg", "jpeg", "png", "cin", "tiff", "tif", "hdr", "hdri"]
    movie_exts = ["mov", "mxf", "mp4", "avi"]
    supported_exts = image_exts + movie_exts

    # Configurable by Settings
    profiles = None

    def process(self, instance):

        self.log.debug(self.profiles)
        self.log.debug(f"{self.label} was run!")