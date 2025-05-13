from . import api
from pipeline.h.heuristics import echoheuristic


class Adapter(api.Heuristic):
    """
    Adapter is a base class for heuristic adapters.
    """
    def __init__(self, heuristic):
        # if the heuristic is not callable, such as when it's a Python
        # primitive, wrap it in an EchoHeuristic
        if not callable(heuristic):
            heuristic = echoheuristic.EchoHeuristic(heuristic)

        self._adaptee = heuristic 
