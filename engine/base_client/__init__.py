from engine.base_client.client import BaseClient
from engine.base_client.configure import BaseConfigurator
from engine.base_client.search import BaseSearcher
from engine.base_client.upload import BaseUploader


class IncompatibilityError(Exception):
    def __init__(self, message=None):
        super().__init__(message or "Incompatible engine/dataset parameters")


__all__ = [
    "BaseClient",
    "BaseConfigurator",
    "BaseSearcher",
    "BaseUploader",
    "IncompatibilityError",
]
