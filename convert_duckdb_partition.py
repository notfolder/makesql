import duckdb
import os
import glob
import zipfile
from tqdm import tqdm
import time
import pandas as pd

def convert_parquet_to_wide_csv_zip(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv', chunk_size=10):
    # serial/serial_subのディレクトリを探索
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', 'data.parquet'))
    tmp_csv = output_csv_name
    first = True

    con = duckdb.connect()
    # attr_nameのリストを全体から取得
    parquet_glob = os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', 'data.parquet')
    attr_names = con.execute(f"SELECT DISTINCT attr_name FROM read_parquet('{parquet_glob}')").fetchall()
    attr_names_str = ', '.join([f"'{row[0]}'" for row in attr_names])
    # x, yの範囲を取得
    minmax = con.execute(f"SELECT MIN(x), MAX(x), MIN(y), MAX(y) FROM read_parquet('{parquet_glob}')").fetchone()
    min_x, max_x, min_y, max_y = minmax
    # x, yをchunk_size単位で分割して処理
    x_ranges = [(x, min(x+chunk_size-1, max_x)) for x in range(min_x, max_x+1, chunk_size)]
    y_ranges = [(y, min(y+chunk_size-1, max_y)) for y in range(min_y, max_y+1, chunk_size)]
    total_chunks = len(x_ranges) * len(y_ranges)
    with tqdm(total=total_chunks, desc="chunk単位で変換中") as pbar:
        for x_start, x_end in x_ranges:
            for y_start, y_end in y_ranges:
                query = f'''
                SELECT serial, serial_sub, x, y,
                    *
                FROM (
                    SELECT serial, serial_sub, x, y, attr_name, attr_value
                    FROM read_parquet('{parquet_glob}')
                    WHERE x BETWEEN {x_start} AND {x_end} AND y BETWEEN {y_start} AND {y_end}
                )
                PIVOT (
                    MAX(attr_value) FOR attr_name IN ({attr_names_str})
                )
                '''
                # 最初だけヘッダー付きで書き込み、2回目以降はヘッダーなしで追記
                if first:
                    con.execute(f"COPY ({query}) TO '{tmp_csv}' (HEADER, DELIMITER ',')")
                    first = False
                else:
                    con.execute(f"COPY ({query}) TO '{tmp_csv}' (HEADER FALSE, DELIMITER ',', FORCE_QUOTE *)")
                pbar.update(1)
    con.close()

    # zip圧縮
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(tmp_csv, arcname=os.path.basename(tmp_csv))
    os.remove(tmp_csv)

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    start = time.time()
    convert_parquet_to_wide_csv_zip(parq_root, output_zip)
    elapsed = time.time() - start
    print(f"完了しました！ 経過時間: {elapsed:.2f}秒")
