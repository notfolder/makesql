"""
convert_pyarrow_multi.py の出力結果をテストするモジュール

このテストは convert_pyarrow_multi.py の出力結果の正確性を検証します。
大規模データのため全数テストは実行できませんが、
特定のserial/serial_sub/x/y組み合わせについて値の検証を行います。

テスト内容:
1. 出力形式の検証 (ZIP内のCSVファイル形式)
2. 特定座標での値の正確性確認
3. データの完全性確認
4. Wide format変換の正確性確認

実行方法:
    python test_convert_pyarrow_multi.py

必要な依存関係:
    - pyarrow
    - pandas 
    - numpy
    - dask
    - tqdm
"""

import unittest
import os
import shutil
import tempfile
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path
import dask.dataframe as dd
from convert_pyarrow_multi import convert_parquet_to_wide_csv_zip_pyarrow_chunked


class TestConvertPyarrowMulti(unittest.TestCase):
    """
    convert_pyarrow_multi.pyの出力結果をテストするクラス
    
    大規模データなので全数テストはできないため、
    いくつかのserial/serial_subについて特定のx,y座標の値が
    正しく変換されているかを確認する
    """
    
    def setUp(self):
        """テスト用の一時ディレクトリと小規模テストデータを準備"""
        # 一時ディレクトリの作成
        self.test_dir = tempfile.mkdtemp()
        self.parq_input = os.path.join(self.test_dir, "parq_input")
        self.parq_tmp = os.path.join(self.test_dir, "parq_tmp")
        self.output_zip = os.path.join(self.test_dir, "test_output.zip")
        
        # テスト用の小規模データを生成
        self.generate_test_data()
    
    def tearDown(self):
        """テスト後のクリーンアップ"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def generate_test_data(self):
        """テスト用の小規模データを生成
        
        gen_dummy_db.pyを参考に、テスト用の小さなデータセットを作成:
        - 2個のserial (000000001, 000000002)  
        - 2個のserial_sub (01, 02)
        - 3個のattr (attr1, attr2, attr3)
        - 4個の座標 (x,y) = (1,1), (1,2), (2,1), (2,2)
        """
        os.makedirs(self.parq_input, exist_ok=True)
        
        # テストデータの基本パラメータ
        test_serials = ['000000001', '000000002']
        test_serial_subs = ['01', '02']
        test_attrs = ['attr1', 'attr2', 'attr3']
        test_coordinates = [(1, 1), (1, 2), (2, 1), (2, 2)]
        
        # 期待値を保存（後で検証に使用）
        self.expected_data = {}
        
        all_data = []
        for serial in test_serials:
            for serial_sub in test_serial_subs:
                for attr in test_attrs:
                    for x, y in test_coordinates:
                        # 確実な値を設定（テストしやすくするため）
                        attr_value = float(f"{serial[-1]}{serial_sub}{attr[-1]}{x}{y}")
                        
                        # 期待値を記録
                        key = (serial, serial_sub, x, y, attr)
                        self.expected_data[key] = attr_value
                        
                        all_data.append({
                            'serial': serial,
                            'serial_sub': serial_sub,
                            'x': x,
                            'y': y,
                            'attr_name': attr,
                            'attr_value': attr_value
                        })
        
        # DataFrameを作成してparquetに保存
        df = pd.DataFrame(all_data)
        df = df.astype({
            'serial': 'str',
            'serial_sub': 'str',
            'x': 'int32',
            'y': 'int32',
            'attr_name': 'str',
            'attr_value': 'float64'
        })
        
        # Daskを使用してパーティション分割してparquet保存
        ddf = dd.from_pandas(df, npartitions=1)
        ddf.to_parquet(
            self.parq_input,
            engine='pyarrow',
            write_index=False,
            partition_on=['serial', 'serial_sub']
        )
        
        print(f"テストデータを生成しました: {len(all_data)}レコード")
    
    def test_convert_output_format(self):
        """変換処理の実行と出力形式のテスト"""
        # convert_pyarrow_multi.pyの処理を実行
        convert_parquet_to_wide_csv_zip_pyarrow_chunked(
            self.parq_input, 
            self.parq_tmp, 
            self.output_zip
        )
        
        # 出力ファイルが存在することを確認
        self.assertTrue(os.path.exists(self.output_zip), "出力ZIPファイルが生成されていません")
        
        # ZIPファイルの中身を確認
        with zipfile.ZipFile(self.output_zip, 'r') as zipf:
            files_in_zip = zipf.namelist()
            self.assertIn('wide.csv', files_in_zip, "ZIPファイル内にwide.csvが含まれていません")
            
            # CSVファイルを読み込み
            with zipf.open('wide.csv') as csv_file:
                df_output = pd.read_csv(csv_file)
        
        print(f"出力CSVの形状: {df_output.shape}")
        print("出力CSVの列:", list(df_output.columns))
        
        # 基本的な形式チェック
        expected_columns = ['serial', 'serial_sub', 'x', 'y', 'attr1', 'attr2', 'attr3']
        for col in expected_columns:
            self.assertIn(col, df_output.columns, f"列'{col}'が出力に含まれていません")
        
        # テストで使用するためにインスタンス変数に保存
        self.df_output = df_output
    
    def test_specific_values_accuracy(self):
        """特定のserial/serial_sub/x/yに対する値の正確性をテスト"""
        # まず基本的な出力形式テストを実行
        self.test_convert_output_format()
        df_output = self.df_output
        
        # テストケースを定義 (出力データの実際の形式に合わせて調整)
        test_cases = [
            (1, 1, 1, 1),  # serial=1 (000000001), serial_sub=1 (01), x=1, y=1
            (1, 1, 1, 2),  # serial=1 (000000001), serial_sub=1 (01), x=1, y=2  
            (2, 2, 2, 1),  # serial=2 (000000002), serial_sub=2 (02), x=2, y=1
            (2, 2, 2, 2),  # serial=2 (000000002), serial_sub=2 (02), x=2, y=2
        ]
        
        for serial_int, serial_sub_int, x, y in test_cases:
            # 該当する行を抽出
            mask = (df_output['serial'] == serial_int) & \
                   (df_output['serial_sub'] == serial_sub_int) & \
                   (df_output['x'] == x) & \
                   (df_output['y'] == y)
            
            matching_rows = df_output[mask]
            self.assertEqual(len(matching_rows), 1, 
                           f"serial={serial_int}, serial_sub={serial_sub_int}, x={x}, y={y}の行が見つかりません")
            
            row = matching_rows.iloc[0]
            
            # 元の文字列形式に戻して期待値を計算
            original_serial = f"{serial_int:09d}"
            original_serial_sub = f"{serial_sub_int:02d}"
            
            # 各属性の値を検証
            for attr_idx, attr in enumerate(['attr1', 'attr2', 'attr3'], 1):
                expected_key = (original_serial, original_serial_sub, x, y, attr)
                expected_value = self.expected_data[expected_key]
                actual_value = row[attr]
                
                self.assertAlmostEqual(
                    actual_value, expected_value, places=6,
                    msg=f"値が一致しません - serial={serial_int}({original_serial}), "
                        f"serial_sub={serial_sub_int}({original_serial_sub}), "
                        f"x={x}, y={y}, attr={attr}: 期待値={expected_value}, 実際値={actual_value}"
                )
        
        print("特定の値の検証が完了しました")
    
    def test_data_completeness(self):
        """データの完全性テスト - 全ての期待されるレコードが存在するか"""
        # まず基本的な出力形式テストを実行
        self.test_convert_output_format()
        df_output = self.df_output
        
        # 期待される行数 (serial×serial_sub×x×y の組み合わせ数)
        expected_rows = 2 * 2 * 4  # 2 serials × 2 serial_subs × 4 coordinates
        
        self.assertEqual(len(df_output), expected_rows,
                        f"期待される行数{expected_rows}と実際の行数{len(df_output)}が一致しません")
        
        # 出力形式に合わせて検証 (整数値での組み合わせ)
        expected_serial_values = {1, 2}
        expected_serial_sub_values = {1, 2}
        expected_x_values = {1, 2}
        expected_y_values = {1, 2}
        
        actual_serial_values = set(df_output['serial'].unique())
        actual_serial_sub_values = set(df_output['serial_sub'].unique())
        actual_x_values = set(df_output['x'].unique())
        actual_y_values = set(df_output['y'].unique())
        
        self.assertEqual(expected_serial_values, actual_serial_values,
                        "serialの値が期待と一致しません")
        self.assertEqual(expected_serial_sub_values, actual_serial_sub_values,
                        "serial_subの値が期待と一致しません")
        self.assertEqual(expected_x_values, actual_x_values,
                        "xの値が期待と一致しません")
        self.assertEqual(expected_y_values, actual_y_values,
                        "yの値が期待と一致しません")
        
        print("データの完全性検証が完了しました")
    
    def test_wide_format_conversion(self):
        """縦横変換（pivot）が正しく動作しているかのテスト"""
        # まず基本的な出力形式テストを実行
        self.test_convert_output_format()
        df_output = self.df_output
        
        # wide formatの検証 - NaN値がないことを確認
        attr_columns = ['attr1', 'attr2', 'attr3']
        for attr_col in attr_columns:
            null_count = df_output[attr_col].isnull().sum()
            self.assertEqual(null_count, 0, 
                           f"属性列'{attr_col}'にNaN値があります: {null_count}個")
        
        # 各行が唯一のserial/serial_sub/x/yの組み合わせを持つことを確認
        index_cols = ['serial', 'serial_sub', 'x', 'y']
        unique_combinations = df_output[index_cols].drop_duplicates()
        
        self.assertEqual(len(unique_combinations), len(df_output),
                        "重複するserial/serial_sub/x/yの組み合わせが存在します")
        
        print("Wide format変換の検証が完了しました")


if __name__ == '__main__':
    unittest.main()