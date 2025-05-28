import vaex
import os
import glob
import zipfile
import time
import pandas as pd

def convert_parquet_to_wide_csv_zip_vaex(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    t_start = time.time()
    print(f"[TIMER] 処理開始: {time.strftime('%Y-%m-%d %H:%M:%S')} (UNIX: {t_start:.2f})")
    print("[TIMER] parquetファイル探索開始")
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet'))
    print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    t1 = time.time()
    print("[TIMER] vaexでparquet一括読み込み開始")
    df = vaex.open(parquet_files)
    print(f"[TIMER] vaexでparquet一括読み込み完了, 経過: {time.time()-t1:.2f}秒")

    t2 = time.time()
    print("[TIMER] pivot処理開始（前処理なし）")
    pdf = df.to_pandas_df()
    wide = pdf.pivot_table(index=["x", "y"], columns="attr_name", values="attr_value", aggfunc="first").reset_index()
    print(f"[TIMER] pivot処理完了, 経過: {time.time()-t2:.2f}秒")

    t3 = time.time()
    print("[TIMER] CSV出力開始")
    wide.to_csv(output_csv_name, index=False)
    print(f"[TIMER] CSV出力完了, 経過: {time.time()-t3:.2f}秒")
    t4 = time.time()
    print("[TIMER] zip圧縮開始")
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(output_csv_name, arcname=output_csv_name)
    print(f"[TIMER] zip圧縮完了, 経過: {time.time()-t4:.2f}秒")
    os.remove(output_csv_name)
    print(f"完了: {output_zip_path}")
    elapsed = time.time() - t_start
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_vaex.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    t0 = time.time()
    convert_parquet_to_wide_csv_zip_vaex(parq_root, output_zip)
    t1 = time.time()
    print(f"main関数全体の処理時間: {t1-t0:.2f}秒")
