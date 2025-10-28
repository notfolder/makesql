import pandas as pd
from sqlalchemy import create_engine


def create_summary_pandas(source_db_path, evaluate_expr) -> pd.DataFrame:
    """Pandasを使用して検証用のサマリーDBを作成する関数。evaluate_exprを外部から渡す"""
    from sqlalchemy import create_engine
    import pandas as pd
    engine = create_engine(f'sqlite:///{source_db_path}')
    with engine.connect() as conn:
        df = pd.read_sql_table('value_table', conn)
        pivoted = df.pivot_table(
            index=['serial', 'serial_sub', 'x', 'y'],
            columns='attr_name',
            values='attr_value',
            aggfunc='first'
        ).reset_index()
        # 式の評価関数を外部から渡す
        pivoted['calculated_value'] = evaluate_expr(pivoted)
        # x, y単位で集約
        summary_xy = pivoted.groupby(['serial', 'serial_sub', 'x', 'y']).agg({
            'calculated_value': [
                'max',
                lambda x: x.quantile(0.75),
                'median',
                lambda x: x.quantile(0.25),
                'min'
            ]
        }).reset_index()
        summary_xy.columns = ['serial', 'serial_sub', 'x', 'y', 'max', 'q3', 'median', 'q1', 'min']
        # serial, serial_sub単位で再集約
        summary = summary_xy.groupby(['serial', 'serial_sub']).agg({
            'max': 'max',
            'q3': 'max',
            'median': 'max',
            'q1': 'max',
            'min': 'min'
        }).reset_index()
        return summary


def compare_results(db1_path, db2_path):
    """2つのサマリーDBの結果を比較する関数"""
    engine1 = create_engine(f'sqlite:///{db1_path}')
    engine2 = create_engine(f'sqlite:///{db2_path}')
    
    # engineからconnectionを取得して使用
    with engine1.connect() as conn1, engine2.connect() as conn2:
        df1 = pd.read_sql_table('summary_table', conn1)
        df2 = pd.read_sql_table('summary_table', conn2)
    
    return pd.testing.assert_frame_equal(df1, df2)

