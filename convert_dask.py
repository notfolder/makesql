import pandas as pd
import os
import glob
import zipfile
from tqdm import tqdm
import io
import dask.dataframe as dd
import time

def convert_parquet_to_wide_csv_zip_dask(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    t_start = time.time()
    print("[TIMER] parquetファイル探索開始")
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet'))
    print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    t1 = time.time()
    print("[TIMER] Daskでparquet読み込み開始")
    ddf = dd.read_parquet(parquet_files, engine='pyarrow')
    print(f"[TIMER] Daskでparquet読み込み完了, 経過: {time.time()-t1:.2f}秒")

    t2 = time.time()
    print("[TIMER] 欠損除外・型変換開始")
    ddf = ddf.dropna(subset=['x', 'y', 'attr_name', 'attr_value'])
    ddf['serial'] = ddf['serial'].astype(str)
    print(f"[TIMER] 欠損除外・型変換完了, 経過: {time.time()-t2:.2f}秒")

    t3 = time.time()
    print("[TIMER] attr_name一覧取得開始 (最初のserialのみで取得)")
    first_serial = ddf['serial'].head(1, compute=True)[0]
    serial1_ddf = ddf[ddf['serial'] == first_serial]
    attr_names = serial1_ddf['attr_name'].drop_duplicates().compute().tolist()
    attr_names = sorted(attr_names)
    print(f"[TIMER] attr_name一覧取得完了, 経過: {time.time()-t3:.2f}秒 (serial={first_serial}の件数: {len(attr_names)})")

    meta_cols = ['x', 'y'] + attr_names
    meta_df = pd.DataFrame(columns=meta_cols)

    def pivot_partition(df):
        wide = df.pivot_table(index=['x', 'y'], columns='attr_name', values='attr_value', aggfunc='first').reset_index()
        for col in attr_names:
            if col not in wide.columns:
                wide[col] = pd.NA
        wide = wide[['x', 'y'] + attr_names]
        return wide

    t4 = time.time()
    print("[TIMER] Daskでpivot処理開始")
    wide_ddf = ddf.map_partitions(pivot_partition, meta=meta_df)
    print(f"[TIMER] Daskでpivot処理完了, 経過: {time.time()-t4:.2f}秒")

    import tempfile
    t5 = time.time()
    print("[TIMER] 分割CSV出力開始")
    with tempfile.TemporaryDirectory() as tmpdir:
        wide_ddf.to_csv(os.path.join(tmpdir, 'part_*.csv'), index=False, single_file=False)
        print(f"[TIMER] 分割CSV出力完了, 経過: {time.time()-t5:.2f}秒")
        t6 = time.time()
        print("[TIMER] CSV連結・zip圧縮開始")
        csv_files = sorted(glob.glob(os.path.join(tmpdir, 'part_*.csv')))
        with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            with zipf.open(output_csv_name, 'w', force_zip64=True) as zf:
                import io
                with io.TextIOWrapper(zf, encoding='utf-8', newline='') as writer:
                    for i, fname in enumerate(csv_files):
                        with open(fname, 'r', encoding='utf-8') as infile:
                            if i == 0:
                                writer.write(infile.read())
                            else:
                                infile.readline()  # skip header
                                writer.write(infile.read())
        print(f"[TIMER] CSV連結・zip圧縮完了, 経過: {time.time()-t6:.2f}秒")
    print(f"完了: {output_zip_path}")
    elapsed = time.time() - t_start
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    import time
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_dask.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    t0 = time.time()
    convert_parquet_to_wide_csv_zip_dask(parq_root, output_zip)
    t1 = time.time()
    print(f"main関数全体の処理時間: {t1-t0:.2f}秒")
