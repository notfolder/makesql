import unittest
from make_sql import create_summary_sql, create_summary_pandas, compare_results
from sqlalchemy import create_engine
import pandas as pd

class TestMakeSql(unittest.TestCase):
    def setUp(self):
        # テスト用のデータベースをセットアップ
        self.test_db = 'test_dummy_db.sqlite'
        self.engine = create_engine(f'sqlite:///{self.test_db}')
        
        # テストデータの作成
        test_data = pd.DataFrame({
            'serial': ['000000001'] * 4,
            'serial_sub': ['01'] * 4,
            'x': [1, 1, 2, 2],
            'y': [1, 2, 1, 2],
            'attr_name': ['attr1', 'attr2', 'attr1', 'attr2'],
            'attr_value': [1.0, 2.0, 3.0, 4.0]
        })
        test_data.to_sql('value_table', self.engine, if_exists='replace', index=False)

    def test_summary_calculation(self):
        expr = "MEAN(MIN(MAX(attr1,attr2),10), attr1, attr2, attr3)"
        
        # SQLAlchemyによる計算
        query = create_summary_sql(expr, self.test_db)
        
        # Pandasによる計算（検証用）
        create_summary_pandas(expr, self.test_db, 'test_summary.sqlite')
        
        # 結果の比較
        self.assertIsNone(compare_results('test_summary.sqlite', 'test_summary_pandas.sqlite'))

if __name__ == '__main__':
    unittest.main()