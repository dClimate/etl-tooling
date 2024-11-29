import re

import orjson

from .combine import CombinePreprocessor


def fix_fill_value(fill_value) -> CombinePreprocessor:
    """Preprocessor to set the fill value.

    Some inputs have fill values which are different the missing value or which are different values in different
    places, either of which can cause problems. This preprocessor will set the fill value in the zarr json to the
    specified value.

    Parameters
    ----------
    fill_value :
        The fill value to set.

    Returns
    -------
    Preprocessor :
        A preprocessor instance that can set the fill value.
    """

    def fix_fill_value(refs, **kwargs):
        ref_names = set()
        file_match_pattern = "(.*?)/"
        for ref in refs:
            if re.match(file_match_pattern, ref) is not None:
                ref_names.add(re.match(file_match_pattern, ref).group(1))

        for ref in ref_names:
            fill_value_fix = orjson.loads(refs[f"{ref}/.zarray"])
            fill_value_fix["fill_value"] = fill_value
            refs[f"{ref}/.zarray"] = orjson.dumps(fill_value_fix)

        return refs

    return fix_fill_value
