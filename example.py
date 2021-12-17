import typing as t
import os
from functools import partial
import time
import sys

import pandas as pd

from df_processor import DFProcessor
from df_processor.utils import timer


def count_rows(df: t.Union[pd.Series, pd.DataFrame], test_message: str) -> int:
    pid = os.getpid()
    rows = df.shape[0]
    time.sleep(5)
    print(
        f"PID: {pid}; DF Size: {sys.getsizeof(df)}; "
        f"Rows: {rows}; Test message: {test_message}"
    )
    return rows


@timer
def main() -> int:
    df_processor = DFProcessor(worker_queue_size=5, n_workers=6)

    df = pd.read_csv("/Users/etitov1/Downloads/sample.csv", sep=",")
    df = pd.concat([df] * 25_000, ignore_index=True)
    print(f"Main thread process: {os.getpid()}; DF size: {sys.getsizeof(df)}")

    results = df_processor.process_df(
        df=df,
        func=partial(count_rows, test_message="KEK"),
        n_partitions=200,
    )
    print("Result:", sum(results))
    return 0


if __name__ == "__main__":
    main()
