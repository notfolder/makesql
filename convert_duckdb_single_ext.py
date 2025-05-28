import duckdb
import os
import glob
import zipfile
import time

def convert_parquet_to_wide_csv_zip_ext(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv', memory_limit='2GB', temp_dir='duckdb_tmp'):
    start = time.time()
    print(f"[TIMER] 処理開始: {time.strftime('%Y-%m-%d %H:%M:%S')} (UNIX: {start:.2f})")
    # すべてのparquetファイルを一括で読み込み
    parquet_glob = os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet')
    tmp_csv = output_csv_name

    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect()
    # 外部メモリモード設定
    con.execute(f"PRAGMA memory_limit='{memory_limit}'")
    con.execute(f"PRAGMA temp_directory='{os.path.abspath(temp_dir)}'")

    # attr_nameのリストを一括で取得
    attr_names = con.execute(f"SELECT DISTINCT attr_name FROM read_parquet('{parquet_glob}')").fetchall()
    attr_names_str = ', '.join([f"'{row[0]}'" for row in attr_names])
    query = f'''
    SELECT serial, serial_sub, x, y,
        *
    FROM (
        SELECT serial, serial_sub, x, y, attr_name, attr_value
        FROM read_parquet('{parquet_glob}')
    )
    PIVOT (
        MAX(attr_value) FOR attr_name IN ({attr_names_str})
    )
    '''
    # 一括でCSV出力
    t1 = time.time()
    print(f"[TIMER] DuckDB PIVOT+CSV出力開始")
    con.execute(f"COPY ({query}) TO '{tmp_csv}' (HEADER, DELIMITER ',')")
    con.close()
    print(f"[TIMER] DuckDB PIVOT+CSV出力完了, 経過: {time.time()-t1:.2f}秒")

    t2 = time.time()
    print(f"[TIMER] zip圧縮開始")
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(tmp_csv, arcname=os.path.basename(tmp_csv))
    print(f"[TIMER] zip圧縮完了, 経過: {time.time()-t2:.2f}秒")
    os.remove(tmp_csv)
    elapsed = time.time() - start
    print(f"完了: {output_zip_path}")
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    parq_root = 'parq_output'
    output_zip = 'wide_csvs_ext.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    start = time.time()
    convert_parquet_to_wide_csv_zip_ext(parq_root, output_zip)
    elapsed = time.time() - start
    print(f"完了しました！ 経過時間: {elapsed:.2f}秒")
