import pyblish.api


from openpype.pipeline import publish


class ExtractEffectOCIO(publish.Extractor):
    """
        Extracts OCIO config files from effect stack.
        Upon extraction it also updates the representation
        to also feature an effectOcio entry to be published
    """

    label = "Extract Effect OCIO (TTD)"
    order = pyblish.api.ExtractorOrder

    profiles = None

    def process(self, instance):

        self.log.debug(self.profiles)
        self.log.debug(f"{self.label} was run!")