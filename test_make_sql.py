import unittest
from make_sql import create_summary_sql, make_df_sql
from sqlalchemy import create_engine
import pandas as pd
from create_summary_pandas import create_summary_pandas, compare_results

class TestMakeSql(unittest.TestCase):
    def setUp(self):
        # テスト用のデータベースをセットアップ
        self.test_db = 'test_dummy_db.sqlite'
        self.engine = create_engine(f'sqlite:///{self.test_db}')
        
        # テストデータの作成
        test_data = pd.DataFrame({
            'serial': ['000000001'] * 6,
            'serial_sub': ['01'] * 6,
            'x': [1, 1, 2, 2, 3, 3],
            'y': [1, 2, 3, 1, 2, 3],
            'attr_name': ['attr1', 'attr2', 'attr3', 'attr1', 'attr2', 'attr3'],
            'attr_value': [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        })
        test_data.to_sql('value_table', self.engine, if_exists='replace', index=False)

    def test_summary_calculation(self):
        expr = "MEAN(MIN(MAX(attr1,attr2),10), attr1, attr2, attr3)"
        # Pandas用の式ロジック
        def evaluate_expr(df):
            import numpy as np
            max12 = np.maximum(df['attr1'], df['attr2'])
            min_max10 = np.minimum(max12, 10)
            vals = [min_max10, df['attr1'], df['attr2'], df['attr3']]
            return np.mean(vals, axis=0)
        # SQLAlchemyによる計算
        df_sql = make_df_sql(expr, self.test_db)
        # Pandasによる計算（検証用）
        df_pandas = create_summary_pandas(self.test_db, evaluate_expr)
        # 結果の比較
        pd.testing.assert_frame_equal(df_sql, df_pandas)

if __name__ == '__main__':
    unittest.main()