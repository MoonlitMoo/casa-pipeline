from typing import List, NewType, Tuple, Union

ClusteringResult = NewType('ClusteringResult', Tuple[int, List[List[Union[int, bool]]], List[int], List[List[Union[int, float, bool]]]])
DetectedLineList = NewType('DetectedLineList', List[List[Union[int, float, bool]]])
LineProperty = NewType('LineProperty', List[Union[float, bool]])
LineWindow = NewType('LineWindow', Union[str, dict, List[int], List[float], List[str]])
