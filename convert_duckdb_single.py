import duckdb
import os
import glob
import zipfile
import time

def convert_parquet_to_wide_csv_zip(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    # すべてのparquetファイルを一括で読み込み
    parquet_glob = os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet')
    tmp_csv = output_csv_name

    con = duckdb.connect()
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
    con.execute(f"COPY ({query}) TO '{tmp_csv}' (HEADER, DELIMITER ',')")
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
