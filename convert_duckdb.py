import duckdb
import os
import glob
import zipfile
from tqdm import tqdm
import time

def convert_parquet_to_wide_csv_zip(parq_root_dir, output_zip_path, output_csv_name='all_serials.csv'):
    # serial/serial_subのディレクトリを探索
    parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', 'data.parquet'))
    tmp_csv = output_csv_name
    first = True

    con = duckdb.connect()
    for pq_path in tqdm(parquet_files, desc="parquet変換中"):
        # parquetを直接読み込み、pivotしてワイド化
        # attr_nameのリストを取得
        attr_names = con.execute(f"SELECT DISTINCT attr_name FROM read_parquet('{pq_path}')").fetchall()
        attr_names = [f"'{row[0]}" for row in attr_names]
        attr_names_str = ', '.join([f"'{row[0]}'" for row in con.execute(f"SELECT DISTINCT attr_name FROM read_parquet('{pq_path}')").fetchall()])
        query = f'''
        SELECT x, y,
            *
        FROM (
            SELECT x, y, attr_name, attr_value
            FROM read_parquet('{pq_path}')
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
