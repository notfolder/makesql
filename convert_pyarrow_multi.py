import os
import glob
import zipfile
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import time
import io


def convert_parquet_to_wide_csv_zip_pyarrow_chunked(parq_root_dir, output_zip_path):
    """
    serial,serial_subごとにx,yのmod 2で4分割し、逐次処理でwide csvをzipに格納
    """
    t_start = time.time()
    print("[TIMER] parquetファイル探索開始")
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet'))
    print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")
    if not parquet_files:
        print('parquetファイルが見つかりません')
        return

    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for pq_path in parquet_files:
            # serial, serial_subをパスから抽出
            parts = pq_path.split(os.sep)
            serial = [p for p in parts if p.startswith('serial=')][0].split('=')[1]
            serial_sub = [p for p in parts if p.startswith('serial_sub=')][0].split('=')[1]

            # ParquetFileでrow_groupごとに部分的に読み込み、分割ごとに即座にzipへ追記
            pf = pq.ParquetFile(pq_path)
            num_row_groups = pf.num_row_groups
            for rg in range(num_row_groups):
                table = pf.read_row_group(rg)
                df = table.to_pandas()
                df = df.dropna(subset=['x', 'y', 'attr_name', 'attr_value'])
                df['serial'] = df['serial'].astype(str)
                # 4分割 (x%2, y%2)
                for x_mod in [0, 1]:
                    for y_mod in [0, 1]:
                        df_chunk = df[(df['x'] % 2 == x_mod) & (df['y'] % 2 == y_mod)]
                        if len(df_chunk) == 0:
                            continue
                        attr_names = sorted(df_chunk['attr_name'].drop_duplicates().tolist())
                        wide = df_chunk.pivot_table(index=['x', 'y'], columns='attr_name', values='attr_value', aggfunc='first').reset_index()
                        for col in attr_names:
                            if col not in wide.columns:
                                wide[col] = pd.NA
                        wide = wide[['x', 'y'] + attr_names]
                        # 出力ファイル名例: serial=000000000_serial_sub=01_mod00.csv
                        csv_name = f"serial={serial}_serial_sub={serial_sub}_mod{x_mod}{y_mod}.csv"
                        # zip内で追記（'a'モードはzipfileでサポートされないため、都度header制御）
                        write_header = True
                        if zipf.fp and csv_name in zipf.namelist():
                            write_header = False
                        with zipf.open(csv_name, 'a' if not write_header else 'w') as zf:
                            with io.TextIOWrapper(zf, encoding='utf-8', newline='') as writer:
                                wide.to_csv(writer, index=False, header=write_header)
    print(f"完了: {output_zip_path}")
    elapsed = time.time() - t_start
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_pyarrow_chunked.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    t0 = time.time()
    convert_parquet_to_wide_csv_zip_pyarrow_chunked(parq_root, output_zip)
    t1 = time.time()
    print(f"main関数全体の処理時間: {t1-t0:.2f}秒")
