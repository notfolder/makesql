"""
convert_pyarrow_multi.py の出力ファイルを検証するテストモジュール

このテストは convert_pyarrow_multi.py によって生成された出力ファイルの
正確性を検証します。変換処理自体は実行せず、既存の出力ファイルを
開いて内容を確認します。

テスト内容:
1. 出力形式の検証 (ZIP内のCSVファイル形式)
2. データの整合性確認
3. Wide format形式の正確性確認
4. 必要な列の存在確認

実行方法:
    python test_convert_pyarrow_multi.py

必要な依存関係:
    - pandas 
    - numpy
"""

import unittest
import os
import zipfile
import pandas as pd
import numpy as np
from pathlib import Path


class TestConvertPyarrowMulti(unittest.TestCase):
    """
    convert_pyarrow_multi.pyの出力ファイルを検証するクラス
    
    既存の出力ZIPファイルを開いて、データの正確性と整合性を確認します。
    """
    
    def setUp(self):
        """テスト用の設定"""
        # デフォルトの出力ファイルパス（convert_pyarrow_multi.pyが生成するファイル）
        self.default_output_files = [
            'wide_csvs_pyarrow_chunked.zip',
            'wide.zip',
            'output.zip'
        ]
        
        # 実際に存在する出力ファイルを探す
        self.output_zip = None
        for file_path in self.default_output_files:
            if os.path.exists(file_path):
                self.output_zip = file_path
                break
        
        # 環境変数からも指定可能
        env_output_path = os.environ.get('CONVERT_OUTPUT_ZIP')
        if env_output_path and os.path.exists(env_output_path):
            self.output_zip = env_output_path
    
    def load_output_csv(self):
        """出力ZIPファイルからCSVデータを読み込む"""
        if self.output_zip is None:
            self.skipTest("出力ZIPファイルが見つかりません。convert_pyarrow_multi.pyを実行してから再度実行してください。")
        
        with zipfile.ZipFile(self.output_zip, 'r') as zipf:
            files_in_zip = zipf.namelist()
            if 'wide.csv' not in files_in_zip:
                self.fail(f"ZIPファイル '{self.output_zip}' 内にwide.csvが含まれていません。利用可能ファイル: {files_in_zip}")
            
            with zipf.open('wide.csv') as csv_file:
                df = pd.read_csv(csv_file)
        
        return df
    
    
    def test_output_file_exists_and_readable(self):
        """出力ZIPファイルの存在と読み込み可能性をテスト"""
        self.assertIsNotNone(self.output_zip, 
                           "出力ZIPファイルが見つかりません。convert_pyarrow_multi.pyを実行してから再度実行してください。")
        
        self.assertTrue(os.path.exists(self.output_zip), 
                       f"指定された出力ファイル '{self.output_zip}' が存在しません")
        
        # ZIPファイルとして有効か確認
        try:
            with zipfile.ZipFile(self.output_zip, 'r') as zipf:
                files_in_zip = zipf.namelist()
                self.assertIn('wide.csv', files_in_zip, 
                            f"ZIPファイル内にwide.csvが含まれていません。利用可能ファイル: {files_in_zip}")
        except zipfile.BadZipFile:
            self.fail(f"'{self.output_zip}' は有効なZIPファイルではありません")
        
        print(f"出力ファイルが確認されました: {self.output_zip}")
    
    def test_csv_structure_and_columns(self):
        """CSVファイルの構造と列の検証"""
        df = self.load_output_csv()
        
        print(f"出力CSVの形状: {df.shape}")
        print("出力CSVの列:", list(df.columns))
        
        # 基本的な列の存在確認
        required_columns = ['serial', 'serial_sub', 'x', 'y']
        for col in required_columns:
            self.assertIn(col, df.columns, f"必須列'{col}'が出力に含まれていません")
        
        # 最低1行はデータが存在することを確認
        self.assertGreater(len(df), 0, "CSVファイルにデータが含まれていません")
        
        # serial, serial_subが適切な形式であることを確認（数値または文字列）
        serial_values = df['serial'].dropna()
        self.assertGreater(len(serial_values), 0, "serial列に有効な値がありません")
        
        serial_sub_values = df['serial_sub'].dropna()
        self.assertGreater(len(serial_sub_values), 0, "serial_sub列に有効な値がありません")
        
        # x, yが数値であることを確認
        self.assertTrue(pd.api.types.is_numeric_dtype(df['x']), "x列が数値型ではありません")
        self.assertTrue(pd.api.types.is_numeric_dtype(df['y']), "y列が数値型ではありません")
        
        print("CSVの構造検証が完了しました")
    
    def test_wide_format_structure(self):
        """Wide format形式の検証"""
        df = self.load_output_csv()
        
        # インデックス列（serial, serial_sub, x, y）以外が属性列であることを確認
        index_columns = ['serial', 'serial_sub', 'x', 'y']
        attr_columns = [col for col in df.columns if col not in index_columns]
        
        self.assertGreater(len(attr_columns), 0, "属性列が存在しません")
        print(f"検出された属性列: {attr_columns}")
        
        # 各属性列が数値型であることを確認
        for attr_col in attr_columns:
            if df[attr_col].notna().any():  # NaN以外の値が存在する場合のみチェック
                # 数値に変換できるかチェック
                try:
                    pd.to_numeric(df[attr_col], errors='coerce')
                except:
                    self.fail(f"属性列'{attr_col}'が数値として解釈できません")
        
        # インデックスの組み合わせが一意であることを確認（Wide formatの要件）
        index_combinations = df[index_columns].drop_duplicates()
        self.assertEqual(len(index_combinations), len(df),
                        f"インデックス列の組み合わせが重複しています。期待: {len(df)}, 実際: {len(index_combinations)}")
        
        print("Wide format構造の検証が完了しました")
    
    def test_data_consistency(self):
        """データの一貫性検証"""
        df = self.load_output_csv()
        
        # 基本的な統計情報を確認
        print("\n=== データの基本統計 ===")
        print(f"総レコード数: {len(df)}")
        print(f"serial数: {df['serial'].nunique()}")
        print(f"serial_sub数: {df['serial_sub'].nunique()}")
        print(f"x座標の範囲: {df['x'].min()} ～ {df['x'].max()}")
        print(f"y座標の範囲: {df['y'].min()} ～ {df['y'].max()}")
        
        # NaNの存在を確認（意図しないNaNがないかチェック）
        index_columns = ['serial', 'serial_sub', 'x', 'y']
        for col in index_columns:
            null_count = df[col].isnull().sum()
            self.assertEqual(null_count, 0, 
                           f"インデックス列'{col}'にNaN値があります: {null_count}個")
        
        # 属性列のNaN状況を報告（これは仕様によって正常な場合もある）
        attr_columns = [col for col in df.columns if col not in index_columns]
        for attr_col in attr_columns:
            null_count = df[attr_col].isnull().sum()
            null_ratio = null_count / len(df) * 100
            print(f"属性列'{attr_col}': NaN数={null_count} ({null_ratio:.1f}%)")
        
        print("データ一貫性の検証が完了しました")
    
    def test_sample_data_values(self):
        """サンプルデータの値を検証"""
        df = self.load_output_csv()
        
        # いくつかのサンプルデータを表示して手動確認用
        print("\n=== サンプルデータ (最初の5行) ===")
        print(df.head())
        
        if len(df) > 10:
            print("\n=== サンプルデータ (ランダム5行) ===")
            print(df.sample(5))
        
        # データの妥当性チェック
        index_columns = ['serial', 'serial_sub', 'x', 'y']
        attr_columns = [col for col in df.columns if col not in index_columns]
        
        if attr_columns:
            # 属性値の範囲チェック（異常値の検出）
            for attr_col in attr_columns:
                non_null_values = df[attr_col].dropna()
                if len(non_null_values) > 0:
                    value_range = non_null_values.max() - non_null_values.min()
                    print(f"属性'{attr_col}': 最小={non_null_values.min():.6f}, 最大={non_null_values.max():.6f}, 範囲={value_range:.6f}")
                    
                    # 異常に大きな値や小さな値がないかチェック
                    self.assertTrue(non_null_values.min() > -1e10, 
                                  f"属性'{attr_col}'に異常に小さな値があります: {non_null_values.min()}")
                    self.assertTrue(non_null_values.max() < 1e10, 
                                  f"属性'{attr_col}'に異常に大きな値があります: {non_null_values.max()}")
        
        print("サンプルデータの検証が完了しました")


if __name__ == '__main__':
    unittest.main()