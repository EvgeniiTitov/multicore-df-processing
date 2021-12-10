import typing as t
import os
from functools import partial

import pandas as pd

from df_processor import DFProcessor


def count_rows(df: t.Union[pd.Series, pd.DataFrame], test_message: str) -> int:
    pid = os.getpid()
    rows = df.shape[0]
    print(f"PID: {pid}; Rows: {rows}; Test message: {test_message}")
    return rows


def main() -> int:
    print("Main thread process:", os.getpid())
    df_processor = DFProcessor(worker_queue_size=5, n_workers=2)
    df_iris = pd.read_csv("/Users/etitov1/Downloads/sample.csv", sep=",")
    results = df_processor.process_df(
        df=df_iris,
        func=partial(count_rows, test_message="KEK"),
        n_partitions=5,
    )
    print("Result:", results)
    return 0


if __name__ == "__main__":
    main()
