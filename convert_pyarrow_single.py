import os
import glob
import zipfile
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import time
import io

def convert_parquet_to_wide_csv_zip_pyarrow_singlefile(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    t_start = time.time()
    print("[TIMER] parquetファイル探索開始")
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet'))
    print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    t1 = time.time()
    print("[TIMER] pyarrowでparquet一括読み込み開始")
    # すべてのparquetファイルをpyarrowでまとめて読み込む
    table = pq.ParquetDataset(parquet_files, use_legacy_dataset=False).read()
    print(f"[TIMER] pyarrowでparquet一括読み込み完了, 経過: {time.time()-t1:.2f}秒")

    t2 = time.time()
    print("[TIMER] pandas DataFrame変換・欠損除外・型変換開始")
    df = table.to_pandas()
    df = df.dropna(subset=['x', 'y', 'attr_name', 'attr_value'])
    df['serial'] = df['serial'].astype(str)
    print(f"[TIMER] pandas DataFrame変換・欠損除外・型変換完了, 経過: {time.time()-t2:.2f}秒")

    t3 = time.time()
    print("[TIMER] attr_name一覧取得開始 (最初のserialのみで取得)")
    first_serial = df['serial'].iloc[0]
    attr_names = sorted(df[df['serial'] == first_serial]['attr_name'].drop_duplicates().tolist())
    print(f"[TIMER] attr_name一覧取得完了, 経過: {time.time()-t3:.2f}秒 (serial={first_serial}の件数: {len(attr_names)})")

    t4 = time.time()
    print("[TIMER] pivot処理開始")
    wide = df.pivot_table(index=['x', 'y'], columns='attr_name', values='attr_value', aggfunc='first').reset_index()
    for col in attr_names:
        if col not in wide.columns:
            wide[col] = pd.NA
    wide = wide[['x', 'y'] + attr_names]
    print(f"[TIMER] pivot処理完了, 経過: {time.time()-t4:.2f}秒")

    t5 = time.time()
    print("[TIMER] CSV出力(zip直書き)開始")
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        with zipf.open(output_csv_name, 'w') as zf:
            with io.TextIOWrapper(zf, encoding='utf-8', newline='') as writer:
                wide.to_csv(writer, index=False)
    print(f"[TIMER] CSV出力(zip直書き)完了, 経過: {time.time()-t5:.2f}秒")
    print(f"完了: {output_zip_path}")
    elapsed = time.time() - t_start
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_pyarrow_singlefile.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    t0 = time.time()
    convert_parquet_to_wide_csv_zip_pyarrow_singlefile(parq_root, output_zip)
    t1 = time.time()
    print(f"main関数全体の処理時間: {t1-t0:.2f}秒")
