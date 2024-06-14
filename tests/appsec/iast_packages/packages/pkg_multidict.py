"""
multidict==6.0.5

https://pypi.org/project/multidict/
"""
from flask import Blueprint
from flask import request

from .utils import ResultResponse


pkg_multidict = Blueprint("package_multidict", __name__)


@pkg_multidict.route("/multidict")
def pkg_multidict_view():
    from multidict import MultiDict

    response = ResultResponse(request.args.get("package_param"))

    try:
        param_value = request.args.get("package_param", "key1=value1&key2=value2")
        items = [item.split("=") for item in param_value.split("&")]
        multi_dict = MultiDict(items)

        result_output = f"MultiDict contents: {dict(multi_dict)}"

        response.result1 = result_output
    except Exception as e:
        response.result1 = f"Error: {str(e)}"

    return response.json()
