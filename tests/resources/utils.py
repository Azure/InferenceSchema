# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------


def ordered(obj):
    """
    sort all items iteratively if nested

    :param obj:
    :type func: nested item | dict or list
    :return:
    :rtype: obj
    """
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj
