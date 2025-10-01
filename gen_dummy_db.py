import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import dask.dataframe as dd
import sqlite3
import random

def generate_dummy_data():
    n_serials = 25  # serial数
    n_positions = 100  # 各serial_subごとの要素数
    n_attrs = 10000     # attr数

    output_dir = "parq_output"
    os.makedirs(output_dir, exist_ok=True)

    total = n_serials * 25 * n_attrs
    with tqdm(total=total, desc="全体進捗", unit="attr") as pbar:
        # 各serial/serial_subごとにデータをまとめてDask DataFrameにappend
        all_ddfs = []
        for serial in range(n_serials):
            serial_str = f"{serial:09d}"
            for serial_sub in range(1, 26):
                serial_sub_str = f"{serial_sub:02d}"
                x_positions = np.random.randint(1, 201, size=n_positions)
                y_positions = np.random.randint(1, 201, size=n_positions)
                data = []
                for attr_idx in range(1, n_attrs + 1):
                    attr_name = f"attr{attr_idx}"
                    attr_values = np.random.normal(0, 1, n_positions)
                    for x, y, attr_value in zip(x_positions, y_positions, attr_values):
                        data.append((serial_str, serial_sub_str, x, y, attr_name, attr_value))
                    pbar.update(1)
                df = pd.DataFrame(data, columns=['serial', 'serial_sub', 'x', 'y', 'attr_name', 'attr_value'])
                df = df.astype({
                    'serial': 'str',
                    'serial_sub': 'str',
                    'x': 'int32',
                    'y': 'int32',
                    'attr_name': 'str',
                    'attr_value': 'float64'
                })
                ddf = dd.from_pandas(df, npartitions=1)
                all_ddfs.append(ddf)
        # 全部まとめてconcat
        all_ddf = dd.concat(all_ddfs)
        all_ddf.to_parquet(
            output_dir,
            engine='pyarrow',
            write_index=False,
            partition_on=['serial', 'serial_sub']
        )

def create_sqlite_dummy_data():
    ATTRS = [f"attr{i}" for i in range(1, 5)]
    SERIALS = [f"{i:09d}" for i in range(1, 4)]
    SERIAL_SUBS = [f"{i:02d}" for i in range(1, 4)]

    conn = sqlite3.connect("dummy_db.sqlite")
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS value_table")
    c.execute("""
    CREATE TABLE value_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial TEXT,
        serial_sub TEXT,
        x INTEGER,
        y INTEGER,
        attr_name TEXT,
        attr_value REAL
    )
    """)

    rows = []
    for serial in SERIALS:
        for serial_sub in SERIAL_SUBS:
            for x in range(2):
                for y in range(2):
                    for attr in ATTRS:
                        value = round(random.uniform(10, 100), 2)
                        rows.append((serial, serial_sub, x, y, attr, value))

    c.executemany("INSERT INTO value_table (serial, serial_sub, x, y, attr_name, attr_value) VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()

    print(f"dummy_db.sqliteに{len(rows)}件のデータを作成しました。")

def main():
    print("データ生成開始...")
    # generate_dummy_data()
    create_sqlite_dummy_data()
    print("完了しました！")

if __name__ == "__main__":
    main()