
import gc
import math
import os
import glob
from pathlib import Path
import zipfile
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd
import time
import io
import concurrent.futures


def bach_select_chip(id: str, batch: pa.RecordBatch, parq_tmp: str):
    """    指定されたRecordBatchから、test_namesとx, yのmod 2でフィルタリングし、
    一時的なparqパーティションディレクトリを作成する。
    
    Args:
        id (str): 一意の識別子
        batch (pa.RecordBatch): 対象のRecordBatch
        parq_tmp (str): 一時ディレクトリのパス
    """
    parq_tmp: Path = Path(parq_tmp)
    dict_convert = {'attr_name': 'test_name', 'attr_value': 'test_value'}

    df = batch.to_pandas()
    df = df.rename(columns=dict_convert)
    for serial in df['serial'].unique():
        for serial_sub in df['serial_sub'].unique():
            # 4分割 (x%2, y%2)
            for x_mod in [0, 1, 2, 3]:
                for y_mod in [0, 1, 2, 3]:
                    df_chunk = df[(df['x'] % 4 == x_mod) & (df['y'] % 4 == y_mod)]
                    if len(df_chunk) == 0:
                        continue
                    tmpdir = parq_tmp.joinpath(f"serial={serial}", f"serial_sub={serial_sub}", f"x_mod={x_mod}", f"y_mod={y_mod}")
                    os.makedirs(tmpdir, exist_ok=True)
                    output_path = tmpdir / f"{id}.parquet"
                    df_chunk.to_parquet(output_path, index=False)


def convert_parquet_to_wide_csv_zip_pyarrow_chunked(parq_root_dir, parq_tmp, output_zip_path):
    """
    serial,serial_subごとにx,yのmod 2で4分割し、逐次処理でwide csvをzipに格納
    """
    t_start = time.time()
    # print("[TIMER] parquetファイル探索開始")
    # parquet_files = glob.glob(os.path.join(parq_root_dir, 'serial=*', 'serial_sub=*', '*.parquet'))
    # print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")
    # if not parquet_files:
    #     print('parquetファイルが見つかりません')
    #     return
    
    dataset = ds.dataset(parq_root_dir, format='parquet', partitioning='hive')

    # test_names = ['a', 'b', 'c']
    # scanner = dataset.scanner(batch_size=10_000, filter=ds.field('attr_name').isin(test_names))
    batch_size = 1000_000
    total_rows = dataset.count_rows()
    num_batches = math.ceil(total_rows/batch_size)
    scanner = dataset.scanner(batch_size=batch_size)

    print(f"[TIMER] chipグループ単位わけ処理開始: {num_batches}件, 経過: {time.time()-t_start:.2f}秒")

    from tqdm import tqdm
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        with tqdm(total=num_batches, desc="chip投入") as pbar_in:
            for i, batch in enumerate(scanner.to_batches()):
                futures.append(executor.submit(bach_select_chip, str(i), batch, parq_tmp))
                pbar_in.update(1)
        with tqdm(total=num_batches, desc="chip完了") as pbar_out:
            for future in concurrent.futures.as_completed(futures):
                future.result()
                pbar_out.update(1)
    gc.collect()  # メモリ解放
    
    print(f"[TIMER] chipグループ単位わけ完了: {num_batches}件, 経過: {time.time()-t_start:.2f}秒")

    print("[TIMER] parquetファイル探索開始")
    parquet_files = glob.glob(os.path.join(parq_tmp, 'serial=*', 'serial_sub=*', 'x_mod=*', 'y_mod=*', '*.parquet'))
    print(f"[TIMER] parquetファイル探索完了: {len(parquet_files)}件, 経過: {time.time()-t_start:.2f}秒")

    print("[TIMER] 縦横変換&ファイル出力開始")

    csv_name = "wide.csv"
    write_header = True
    from tqdm import tqdm
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        with zipf.open(csv_name, 'a' if not write_header else 'w') as zf:
            with io.TextIOWrapper(zf, encoding='utf-8', newline='') as writer:
                with tqdm(total=len(parquet_files), desc="parquet出力") as pbar_out:
                    for pq_path in parquet_files:
                        # ParquetFile全体を一括で読み込み、縦横変換して書き込み
                        pf = pq.ParquetFile(pq_path)
                        df_chunk = pf.read().to_pandas()
                        attr_names = sorted(df_chunk['test_name'].drop_duplicates().tolist())
                        index_cols = [col for col in df_chunk.columns if col not in ['test_name', 'test_value']]
                        wide = df_chunk.pivot_table(index=index_cols, columns='test_name', values='test_value', aggfunc='first').reset_index()
                        for col in attr_names:
                            if col not in wide.columns:
                                wide[col] = pd.NA
                        wide = wide[index_cols + attr_names]

                        wide.to_csv(writer, index=False, header=write_header)
                        write_header = False
                        pbar_out.update(1)

    print(f"完了: {output_zip_path}")
    elapsed = time.time() - t_start
    print(f"[TIMER] 全体処理時間: {elapsed:.2f}秒")

if __name__ == "__main__":
    parq_root = 'parq_output'
    parq_tmp = 'parq_tmp'
    output_zip = 'wide_csvs_pyarrow_chunked.zip'
    print(f"変換開始: {parq_root} → {output_zip}")
    t0 = time.time()
    convert_parquet_to_wide_csv_zip_pyarrow_chunked(parq_root, parq_tmp, output_zip)
    t1 = time.time()
    print(f"main関数全体の処理時間: {t1-t0:.2f}秒")
