import pandas as pd
import os
import glob
import uuid
import random

def generate_serial_sub_add_info(parq_root_dir, output_path):
    """
    Sparkパーティショニングされたparq_output配下のserial/serial_subについて、
    ランダムなserial_idとserial_lot情報を追加したテーブルを生成する
    
    Args:
        parq_root_dir (str): parq_outputディレクトリのパス
        output_path (str): 出力するparquetファイルのパス
    """
    
    # パーティション情報を収集
    partition_info = []
    
    # serial=*ディレクトリを検索
    serial_dirs = glob.glob(os.path.join(parq_root_dir, 'serial=*'))
    
    for serial_dir in sorted(serial_dirs):
        # serialの値を抽出（例: "serial=000000000" → "000000000"）
        serial_value = os.path.basename(serial_dir).split('=')[1]
        
        # serial_sub=*ディレクトリを検索
        serial_sub_dirs = glob.glob(os.path.join(serial_dir, 'serial_sub=*'))
        
        for serial_sub_dir in sorted(serial_sub_dirs):
            # serial_subの値を抽出（例: "serial_sub=01" → "01"）
            serial_sub_value = os.path.basename(serial_sub_dir).split('=')[1]
            
            # parquetファイルが存在するかチェック
            parquet_files = glob.glob(os.path.join(serial_sub_dir, '*.parquet'))
            
            if parquet_files:
                # ランダムなserial_idを生成（UUID4を使用）
                serial_id = str(uuid.uuid4())
                
                # serial_lotを生成（serialにポストフィックス"_lot"を付与）
                serial_lot = f"{serial_value}_lot"
                
                partition_info.append({
                    'serial': serial_value,
                    'serial_sub': serial_sub_value,
                    'serial_id': serial_id,
                    'serial_lot': serial_lot,
                    'partition_path': os.path.relpath(serial_sub_dir, parq_root_dir)
                })
    
    # DataFrameを作成
    df = pd.DataFrame(partition_info)
    
    if len(df) > 0:
        # 出力ディレクトリを作成
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # parquetファイルとして出力
        df.to_parquet(output_path, index=False)
        
        print(f"処理完了: {len(df)}件のserial/serial_sub情報を{output_path}に出力しました")
        print(f"Columns: {list(df.columns)}")
        print("\nサンプルデータ:")
        print(df.head())
        
        return df
    else:
        print("パーティションデータが見つかりませんでした")
        return None

def generate_serial_sub_add_info_with_custom_logic(parq_root_dir, output_path, serial_id_type='uuid'):
    """
    より詳細なオプション付きでserial_sub_add_info テーブルを生成
    
    Args:
        parq_root_dir (str): parq_outputディレクトリのパス
        output_path (str): 出力するparquetファイルのパス
        serial_id_type (str): serial_idの生成方法 ('uuid', 'sequential', 'random_int')
    """
    
    partition_info = []
    serial_id_counter = 1
    
    # serial=*ディレクトリを検索
    serial_dirs = glob.glob(os.path.join(parq_root_dir, 'serial=*'))
    
    for serial_dir in sorted(serial_dirs):
        serial_value = os.path.basename(serial_dir).split('=')[1]
        
        # serial_sub=*ディレクトリを検索
        serial_sub_dirs = glob.glob(os.path.join(serial_dir, 'serial_sub=*'))
        
        for serial_sub_dir in sorted(serial_sub_dirs):
            serial_sub_value = os.path.basename(serial_sub_dir).split('=')[1]
            
            # parquetファイルが存在するかチェック
            parquet_files = glob.glob(os.path.join(serial_sub_dir, '*.parquet'))
            
            if parquet_files:
                # serial_idの生成方法を選択
                if serial_id_type == 'uuid':
                    serial_id = str(uuid.uuid4())
                elif serial_id_type == 'sequential':
                    serial_id = f"serial_id_{serial_id_counter:06d}"
                    serial_id_counter += 1
                elif serial_id_type == 'random_int':
                    serial_id = str(random.randint(1000000, 9999999))
                else:
                    serial_id = str(uuid.uuid4())
                
                # serial_lotを生成
                serial_lot = f"{serial_value}_lot"
                
                partition_info.append({
                    'serial': serial_value,
                    'serial_sub': serial_sub_value,
                    'serial_id': serial_id,
                    'serial_lot': serial_lot,
                    'partition_path': os.path.relpath(serial_sub_dir, parq_root_dir),
                    'parquet_file_count': len(parquet_files)
                })
    
    # DataFrameを作成
    df = pd.DataFrame(partition_info)
    
    if len(df) > 0:
        # 出力ディレクトリを作成
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # parquetファイルとして出力
        df.to_parquet(output_path, index=False)
        
        print(f"処理完了: {len(df)}件のserial/serial_sub情報を{output_path}に出力しました")
        print(f"使用したserial_id生成方法: {serial_id_type}")
        print(f"Columns: {list(df.columns)}")
        print("\nサンプルデータ:")
        print(df.head())
        
        # 統計情報も表示
        print(f"\nUnique serials: {df['serial'].nunique()}")
        print(f"Total serial_sub entries: {len(df)}")
        
        return df
    else:
        print("パーティションデータが見つかりませんでした")
        return None

if __name__ == "__main__":
    # 設定
    parq_root_dir = "./parq_output"
    output_path = "./serial_sub_add_info.parquet"
    
    print("=== Basic版でserial_sub_add_info テーブルを生成 ===")
    df_basic = generate_serial_sub_add_info(parq_root_dir, output_path)
    
    print("\n=== 詳細版でserial_sub_add_info テーブルを生成 ===")
    output_path_detailed = "./serial_sub_add_info_detailed.parquet"
    df_detailed = generate_serial_sub_add_info_with_custom_logic(
        parq_root_dir, 
        output_path_detailed, 
        serial_id_type='sequential'
    )
    
    # 結果ファイルの確認
    if os.path.exists(output_path):
        print(f"\n生成されたファイル: {output_path}")
        print(f"ファイルサイズ: {os.path.getsize(output_path)} bytes")
    
    if os.path.exists(output_path_detailed):
        print(f"\n生成されたファイル: {output_path_detailed}")
        print(f"ファイルサイズ: {os.path.getsize(output_path_detailed)} bytes")