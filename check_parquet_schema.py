import os
import glob
import pandas as pd
from collections import defaultdict

def check_parquet_schema(parq_root_dir):
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', 'data.parquet'))
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    schema_dict = defaultdict(list)
    for pq_path in parquet_files:
        try:
            df = pd.read_parquet(pq_path, engine='pyarrow')
            dtypes = tuple((col, str(dtype)) for col, dtype in df.dtypes.items())
            schema_dict[dtypes].append(pq_path)
        except Exception as e:
            print(f"{pq_path} の読み込みでエラー: {e}")

    print("=== スキーマごとのファイル一覧 ===")
    for i, (schema, files) in enumerate(schema_dict.items(), 1):
        print(f"\n[{i}] スキーマ: {schema}")
        for f in files:
            print(f"  {f}")
    if len(schema_dict) == 1:
        print("\n全ファイルのスキーマは一致しています。")
    else:
        print(f"\nスキーマが異なるグループが {len(schema_dict)} 個見つかりました。")

if __name__ == "__main__":
    parq_root = 'parq_output'
    check_parquet_schema(parq_root)
