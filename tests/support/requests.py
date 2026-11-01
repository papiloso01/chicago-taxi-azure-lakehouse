"""Request settings shared by fixture validation and HTTP tests."""

DEFAULT_LIMIT_PARAMETER = "$limit"
DEFAULT_PAGE_SIZE = 1000


def request_parameters(config):
    parameters = dict(config.get("query_parameters", {}))
    pagination = config.get("pagination", {})
    if pagination is None:
        return parameters
    name = pagination.get("limit_parameter", DEFAULT_LIMIT_PARAMETER)
    size = pagination.get("page_size", DEFAULT_PAGE_SIZE)
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Pagination limit_parameter must be a nonempty string")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError("Pagination page_size must be a positive integer")
    parameters.setdefault(name, str(size))
    return parameters
