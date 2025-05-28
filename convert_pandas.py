import pandas as pd
import os
import glob
import zipfile
from tqdm import tqdm
import io

def convert_parquet_to_wide_csv_zip_pandas(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', 'data.parquet'))
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        with zipf.open(output_csv_name, 'w') as zf:
            with io.TextIOWrapper(zf, encoding='utf-8', newline='') as writer:
                first = True
                for pq_path in tqdm(parquet_files, desc="parquet→csv変換中"):
                    try:
                        df = pd.read_parquet(pq_path)
                        df['serial'] = df['serial'].astype(str)  # 型を強制
                        df = df.dropna(subset=['x', 'y', 'attr_name', 'attr_value'])
                        wide = df.pivot_table(index=['x', 'y'], columns='attr_name', values='attr_value', aggfunc='first').reset_index()
                        wide.to_csv(writer, index=False, header=first)
                        first = False
                    except Exception as e:
                        print(f"{pq_path} の処理でエラー: {e}")
    print(f"完了: {output_zip_path}")

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_pandas.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    convert_parquet_to_wide_csv_zip_pandas(parq_root, output_zip)
