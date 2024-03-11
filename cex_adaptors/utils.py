import pandas as pd


def query_dict(dictionary: dict, query: str, query_env: dict = None) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :param query_env: additional variables for query execution
    :return: queried dictionary
    """
    if not query:
        return dictionary

    df = pd.DataFrame(dictionary).T

    if query_env:
        df = df.query(query, local_dict=query_env)
    else:
        df = df.query(query)

    return df.to_dict(orient="index")


def nested_query_dict(dictionary: dict, key: str, query: str) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :return: queried dictionary
    """
    if not query:
        return dictionary

    new_dict = {outer: inner[key] for outer, inner in dictionary.items() if key in inner}

    queried_dict = query_dict(new_dict, query)
    return {key: dictionary[key] for key in queried_dict.keys()}


def sort_dict(dictionary: dict, ascending: bool = True, num: int = None) -> dict:
    """
    Sort a dictionary by its values
    :param dictionary: dictionary to sort
    :param ascending: ascending or descending
    :param num: number of items to return
    :return: sorted dictionary
    """
    df = pd.DataFrame(dictionary).T.reset_index().sort_values(by="index", ascending=ascending).set_index("index")
    if num:
        df = df.iloc[-num:]
    return df.to_dict(orient="index")
