import pytest

from .utils import string_to_val

test_params = [(r"True", True),
               (r"false", 'false'),
               (r"1,2,3", '1,2,3'),
               (r"(1,2,3)", (1, 2, 3)),
               (r"[1,2,3]", [1, 2, 3]),
               (r"None", None),
               (r"none", 'none'),
               (r"'none'", 'none'),
               (r">12klambda", '>12klambda'),
               (r"&lt;12km", '&lt;12km'),
               (r"[[0,0,1,1],[0,1,0,1]]", [[0, 0, 1, 1], [0, 1, 0, 1]])]


@pytest.mark.parametrize("input_str, expected_value", test_params)
def test_string_to_val(input_str, expected_value):
    """Test string_to_val() from pipeline.infrastructure.utils."""
    assert string_to_val(input_str) == expected_value
