import pyblish.api


from openpype.pipeline import publish


class ExtractTimecode(publish.Extractor):
    """
        Extracts timecode from any supported extension.
        Uses OIIO primarily and FFprobe as fallback
        for movie filetypes.
    """

    label = "Extract Timecode (TTD)"
    order = pyblish.api.ExtractorOrder

    profiles = None

    def process(self, instance):

        self.log.debug(self.profiles)
        self.log.debug(f"{self.label} was run!")