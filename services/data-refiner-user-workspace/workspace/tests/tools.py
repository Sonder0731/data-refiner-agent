from typing import List, Dict

from pyspark.sql import DataFrame


def assert_same_by_dict(df: DataFrame, check_data: List[Dict], id_column="__id__"):
    assert id_column in df.columns
    columns = check_data[0].keys()
    if id_column not in columns:
        raise KeyError(f"Test input dataframe must contains id column: `{id_column}`")
    for element in check_data[1:]:
        if set(columns) != set(element.keys()):
            raise ValueError("Check keys are not the same")
    rdd_dict = [row.asDict() for row in df.collect()]
    for check in check_data:
        check_id = check[id_column]
        for row in rdd_dict:
            if row[id_column] == check_id:
                assert row == check
