import logging

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


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


def get_pnl_from_orders(orders: list, market_type: str, info: dict) -> pd.DataFrame:
    def update_avg_price(last: any, cur: pd.Series) -> float:
        if last is None:  # first order or direction changed
            return cur["executed_price"]
        else:
            if ((last["position"] * cur["position"]) < 0) or (last["position"] == 0):
                return cur["executed_price"]
            elif cur["position"] == 0:
                return 0
            elif (cur["executed_volume"] * last["position"]) > 0:  # same direction
                return (last["avg_price"] * last["position"] + cur["executed_price"] * cur["executed_volume"]) / (
                    last["position"] + cur["executed_volume"]
                )
            else:
                return last["avg_price"]

    def update_unrealized_pnl(cur: pd.Series) -> float:
        if cur["position"] == 0:
            return 0
        return (cur["avg_price"] - cur["executed_price"]) * cur["position"]

    def update_realize_pnl(last: any, cur: pd.Series) -> float:
        if last is None:
            return 0
        else:
            if cur["position"] == 0 or (cur["executed_volume"] * last["position"]) < 0:
                return (last["avg_price"] - cur["executed_price"]) * cur["executed_volume"]
            else:
                return 0

    # start calculating pnl
    df = pd.DataFrame(orders).sort_values("timestamp", ascending=True)
    df = df.loc[df["status"] == "filled"]
    df["executed_volume"] *= info["contract_size"]
    df["position"] = df["executed_volume"].cumsum()

    last = None
    for _, cur in df.iterrows():
        avg_price = update_avg_price(last, cur)
        cur["avg_price"] = avg_price
        unrealize_pnl = update_unrealized_pnl(cur)
        realize_pnl = update_realize_pnl(last, cur)

        df.loc[_, ["avg_price", "unrealized_pnl", "realized_pnl"]] = [avg_price, unrealize_pnl, realize_pnl]

        last = cur

    return df
