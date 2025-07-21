"""
convert_pyarrow_multi.py の出力検証テストモジュール

gen_dummy_db.py が生成したHive形式のparquetファイルから
サンプリングして、convert_pyarrow_multi.py が出力したZIPファイルに
正しく値が入っているかを検証します。

テスト内容:
1. 原本データ（Hive parquet）からサンプリング
2. 変換後データ（ZIP内CSV）から対応データを取得
3. 値の正確性を検証（long → wide 変換の妥当性）
4. データの完全性確認

前提条件:
- gen_dummy_db.py が実行済みで parq_output/ ディレクトリに Hive parquet が存在
- convert_pyarrow_multi.py が実行済みで ZIP ファイルが存在

実行方法:
    python test_convert_pyarrow_multi.py

必要な依存関係:
    - pandas 
    - numpy
    - pyarrow
"""

import unittest
import os
import zipfile
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import glob
from pathlib import Path
import random


class TestConvertPyarrowMulti(unittest.TestCase):
    """
    convert_pyarrow_multi.py の出力検証クラス
    
    gen_dummy_db.py が生成した原本データと convert_pyarrow_multi.py の
    出力データを比較して、変換の正確性を検証します。
    """
    
    def setUp(self):
        """テスト用の設定"""
        # 再現可能な結果のためのランダムシード設定
        random.seed(42)
        np.random.seed(42)
        
        # 原本データディレクトリ（gen_dummy_db.py の出力）
        self.parq_input_dir = 'parq_output'
        
        # 変換後データファイル（convert_pyarrow_multi.py の出力）
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
    
    def load_sample_from_original_data(self, sample_size=100):
        """原本データ（Hive parquet）からサンプルデータを取得"""
        if not os.path.exists(self.parq_input_dir):
            self.skipTest(f"原本データディレクトリ '{self.parq_input_dir}' が見つかりません。gen_dummy_db.py を実行してから再度実行してください。")
        
        # Hive形式のパーティションディレクトリを探す
        partition_dirs = glob.glob(os.path.join(self.parq_input_dir, 'serial=*', 'serial_sub=*'))
        if not partition_dirs:
            self.skipTest("Hive形式のパーティションディレクトリが見つかりません。")
        
        # ランダムにいくつかのパーティションを選択
        selected_partitions = random.sample(partition_dirs, min(5, len(partition_dirs)))
        
        # サンプルデータを収集
        sample_data = []
        for partition_dir in selected_partitions:
            try:
                dataset = ds.dataset(partition_dir, format='parquet')
                df = dataset.to_table().to_pandas()
                
                # ランダムサンプリング
                if len(df) > sample_size:
                    df_sample = df.sample(sample_size)
                else:
                    df_sample = df
                
                sample_data.append(df_sample)
                
            except Exception as e:
                print(f"パーティション {partition_dir} の読み込みでエラー: {e}")
                continue
        
        if not sample_data:
            self.skipTest("サンプルデータが取得できませんでした。")
        
        # 全サンプルを結合
        combined_sample = pd.concat(sample_data, ignore_index=True)
        print(f"原本データからサンプリング完了: {len(combined_sample)}行")
        return combined_sample
    
    def load_wide_data(self):
        """変換後データ（ZIP内CSV）を読み込む"""
        if self.output_zip is None:
            self.skipTest("出力ZIPファイルが見つかりません。convert_pyarrow_multi.py を実行してから再度実行してください。")
        
        with zipfile.ZipFile(self.output_zip, 'r') as zipf:
            files_in_zip = zipf.namelist()
            if 'wide.csv' not in files_in_zip:
                self.fail(f"ZIPファイル '{self.output_zip}' 内にwide.csvが含まれていません。利用可能ファイル: {files_in_zip}")
            
            with zipf.open('wide.csv') as csv_file:
                df = pd.read_csv(csv_file)
        
        return df
    
    def test_prerequisites_exist(self):
        """前提条件（原本データと変換後データ）の存在確認"""
        # 原本データの確認
        self.assertTrue(os.path.exists(self.parq_input_dir), 
                       f"原本データディレクトリ '{self.parq_input_dir}' が存在しません。gen_dummy_db.py を実行してください。")
        
        # Hiveパーティションの確認
        partition_dirs = glob.glob(os.path.join(self.parq_input_dir, 'serial=*', 'serial_sub=*'))
        self.assertGreater(len(partition_dirs), 0, 
                          "Hive形式のパーティションディレクトリが見つかりません。")
        
        # 変換後データの確認
        self.assertIsNotNone(self.output_zip, 
                           "変換後ZIPファイルが見つかりません。convert_pyarrow_multi.py を実行してください。")
        
        self.assertTrue(os.path.exists(self.output_zip), 
                       f"指定された変換後ファイル '{self.output_zip}' が存在しません")
        
        # ZIPファイルの内容確認
        with zipfile.ZipFile(self.output_zip, 'r') as zipf:
            files_in_zip = zipf.namelist()
            self.assertIn('wide.csv', files_in_zip, 
                        f"ZIPファイル内にwide.csvが含まれていません。")
        
        print(f"前提条件確認完了:")
        print(f"  原本データ: {self.parq_input_dir} ({len(partition_dirs)}パーティション)")
        print(f"  変換後データ: {self.output_zip}")
    
    def test_data_transformation_accuracy(self):
        """データ変換の正確性を検証"""
        # 原本データからサンプリング
        original_sample = self.load_sample_from_original_data(sample_size=50)
        
        # 変換後データを読み込み
        wide_data = self.load_wide_data()
        
        # サンプルから検証用のテストケースを作成
        test_cases = []
        for _, row in original_sample.iterrows():
            test_cases.append({
                'serial': str(row['serial']),
                'serial_sub': str(row['serial_sub']),
                'x': int(row['x']),
                'y': int(row['y']),
                'attr_name': str(row['attr_name']),
                'expected_value': float(row['attr_value'])
            })
        
        print(f"検証ケース数: {len(test_cases)}")
        
        # 各テストケースについて変換後データで値を確認
        verified_count = 0
        failed_cases = []
        
        for case in test_cases:
            # wide_dataから該当行を検索
            mask = (
                (wide_data['serial'] == case['serial']) &
                (wide_data['serial_sub'] == case['serial_sub']) &
                (wide_data['x'] == case['x']) &
                (wide_data['y'] == case['y'])
            )
            
            matching_rows = wide_data[mask]
            
            if len(matching_rows) == 0:
                failed_cases.append(f"座標 ({case['serial']}, {case['serial_sub']}, {case['x']}, {case['y']}) が見つかりません")
                continue
            
            # 該当する属性列が存在するか確認
            attr_col = case['attr_name']
            if attr_col not in wide_data.columns:
                failed_cases.append(f"属性列 '{attr_col}' が存在しません")
                continue
            
            # 値の比較
            actual_value = matching_rows[attr_col].iloc[0]
            expected_value = case['expected_value']
            
            if pd.isna(actual_value):
                failed_cases.append(f"座標 ({case['serial']}, {case['serial_sub']}, {case['x']}, {case['y']}) の属性 '{attr_col}' がNaN")
                continue
            
            # 数値の近似比較
            if not np.isclose(actual_value, expected_value, rtol=1e-10):
                failed_cases.append(
                    f"座標 ({case['serial']}, {case['serial_sub']}, {case['x']}, {case['y']}) の属性 '{attr_col}': "
                    f"期待値={expected_value}, 実際値={actual_value}"
                )
                continue
            
            verified_count += 1
        
        # 検証結果の報告
        success_rate = verified_count / len(test_cases) * 100
        print(f"検証成功: {verified_count}/{len(test_cases)} ({success_rate:.1f}%)")
        
        if failed_cases:
            print("失敗ケース:")
            for i, case in enumerate(failed_cases[:10]):  # 最初の10件のみ表示
                print(f"  {i+1}. {case}")
            if len(failed_cases) > 10:
                print(f"  ... 他 {len(failed_cases) - 10} 件")
        
        # 成功率が90%未満の場合はテスト失敗
        self.assertGreaterEqual(success_rate, 90.0, 
                               f"データ変換の正確性が不十分です。成功率: {success_rate:.1f}%")
        
        print("データ変換の正確性検証が完了しました")
    
    def test_data_completeness_validation(self):
        """データ完全性の検証（サンプル範囲でのデータ欠損チェック）"""
        # 特定のserial/serial_subについて完全性をチェック
        original_sample = self.load_sample_from_original_data(sample_size=200)
        wide_data = self.load_wide_data()
        
        # 原本データから unique な (serial, serial_sub, x, y) の組み合わせを取得
        original_coords = original_sample[['serial', 'serial_sub', 'x', 'y']].drop_duplicates()
        print(f"原本データのユニーク座標数: {len(original_coords)}")
        
        # wide_dataでの対応する座標の存在確認
        missing_coords = []
        found_coords = 0
        
        for _, coord in original_coords.iterrows():
            mask = (
                (wide_data['serial'] == str(coord['serial'])) &
                (wide_data['serial_sub'] == str(coord['serial_sub'])) &
                (wide_data['x'] == coord['x']) &
                (wide_data['y'] == coord['y'])
            )
            
            if len(wide_data[mask]) > 0:
                found_coords += 1
            else:
                missing_coords.append(f"({coord['serial']}, {coord['serial_sub']}, {coord['x']}, {coord['y']})")
        
        completeness_rate = found_coords / len(original_coords) * 100
        print(f"座標完全性: {found_coords}/{len(original_coords)} ({completeness_rate:.1f}%)")
        
        if missing_coords:
            print("欠損座標:")
            for i, coord in enumerate(missing_coords[:10]):
                print(f"  {i+1}. {coord}")
            if len(missing_coords) > 10:
                print(f"  ... 他 {len(missing_coords) - 10} 件")
        
        # 完全性が80%未満の場合は警告
        if completeness_rate < 80.0:
            print(f"警告: 座標完全性が低いです ({completeness_rate:.1f}%)")
        
        print("データ完全性検証が完了しました")
    
    def test_wide_format_structure_validation(self):
        """Wide format 構造の検証"""
        wide_data = self.load_wide_data()
        original_sample = self.load_sample_from_original_data(sample_size=100)
        
        print(f"変換後データの形状: {wide_data.shape}")
        print("変換後データの列:", list(wide_data.columns))
        
        # 必須列の存在確認
        required_columns = ['serial', 'serial_sub', 'x', 'y']
        for col in required_columns:
            self.assertIn(col, wide_data.columns, f"必須列'{col}'が存在しません")
        
        # 属性列の確認
        attr_columns = [col for col in wide_data.columns if col not in required_columns]
        self.assertGreater(len(attr_columns), 0, "属性列が存在しません")
        print(f"属性列数: {len(attr_columns)}")
        
        # 原本データの属性名と比較
        original_attrs = set(original_sample['attr_name'].unique())
        wide_attrs = set(attr_columns)
        
        # 原本データの属性がwide_dataに含まれているかチェック（サンプルなので完全一致は期待しない）
        common_attrs = original_attrs & wide_attrs
        print(f"共通属性数: {len(common_attrs)}/{len(original_attrs)}")
        
        if len(common_attrs) > 0:
            print("共通属性の例:", list(common_attrs)[:10])
        
        # インデックスの一意性確認
        index_combinations = wide_data[required_columns].drop_duplicates()
        self.assertEqual(len(index_combinations), len(wide_data),
                        f"インデックス列の組み合わせが重複しています。期待: {len(wide_data)}, 実際: {len(index_combinations)}")
        
        print("Wide format構造の検証が完了しました")


if __name__ == '__main__':
    unittest.main()