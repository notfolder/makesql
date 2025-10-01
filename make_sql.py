from sqlalchemy import create_engine, select, func, text, case, or_, and_
from sqlalchemy.sql import expression
from sqlalchemy.orm import Session
from models import Base, ValueTable, SummaryTable
from lark import Lark
import pandas as pd
import sqlite3
import math
from sqlalchemy.dialects import sqlite

def create_percentile_functions(db_path):
    """SQLiteにパーセンタイル計算用の関数を追加"""
    conn = sqlite3.connect(db_path)
    
    conn.create_aggregate("percentile_25", 1, Percentile25)
    conn.create_aggregate("percentile_50", 1, Percentile50)
    conn.create_aggregate("percentile_75", 1, Percentile75)
    
    conn.close()

class BasePercentile:
    def __init__(self):
        self.values = []
    
    def step(self, value):
        if value is not None:
            self.values.append(float(value))
    
    def finalize(self):
        if not self.values:
            return None
        self.values.sort()
        n = len(self.values)
        if n == 0:
            return None
        
        k = (n - 1) * self.percentile
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return self.values[int(k)]
        
        d0 = self.values[int(f)] * (c - k)
        d1 = self.values[int(c)] * (k - f)
        return d0 + d1

class Percentile25(BasePercentile):
    def __init__(self):
        super().__init__()
        self.percentile = 0.25

class Percentile50(BasePercentile):
    def __init__(self):
        super().__init__()
        self.percentile = 0.5

class Percentile75(BasePercentile):
    def __init__(self):
        super().__init__()
        self.percentile = 0.75

def create_summary_sql(expr_text, source_db_path, parser):
    """SQLAlchemyを使用して集計SQLを生成する関数"""
    # 式をパースしてSQL部分を生成
    tree = parser.parse(expr_text)
    transformer = SQLTransformer()
    expr_sql = transformer.transform(tree)

    # サブクエリとして式の結果を取得（serial, serial_sub, x, y, 計算値）
    subquery = expr_sql.alias('expr_result')

    # メインクエリで統計量を計算
    query = (
        select(
            subquery.c.serial,
            subquery.c.serial_sub,
            func.max(subquery.c.calculated_value).label('max'),
            func.percentile_75(subquery.c.calculated_value).label('q3'),
            func.percentile_50(subquery.c.calculated_value).label('median'),
            func.percentile_25(subquery.c.calculated_value).label('q1'),
            func.min(subquery.c.calculated_value).label('min')
        )
        .select_from(subquery)
        .group_by(subquery.c.serial, subquery.c.serial_sub)
    )
    
    # デバッグ出力
    print("\n---- Generated SQL with values ----")
    compiled = query.compile(
        dialect=sqlite.dialect(),
        compile_kwargs={"literal_binds": True}
    )
    print(str(compiled))
    
    return query

def create_summary_pandas(expr_text, source_db_path, output_db_path):
    """Pandasを使用して検証用のサマリーDBを作成する関数"""
    engine = create_engine(f'sqlite:///{source_db_path}')
    
    # engineからconnectionを取得して使用
    with engine.connect() as conn:
        df = pd.read_sql_table('value_table', conn)
        
        # データをピボット変換して属性名をカラムに（pivot_tableを使用）
        pivoted = df.pivot_table(
            index=['serial', 'serial_sub'],
            columns='attr_name',
            values='attr_value',
            aggfunc='first'  # 重複がある場合は最初の値を使用
        ).reset_index()
        
        # 式の評価関数
        def evaluate_expr(group):
            # MAX(attr1,attr2)+1の場合
            max_val = group[['attr1', 'attr2']].max(axis=1)
            return max_val + 1

        # 式を評価して新しいカラムを作成
        pivoted['calculated_value'] = evaluate_expr(pivoted)
        
        # 統計量を計算
        summary = pivoted.groupby(['serial', 'serial_sub']).agg({
            'calculated_value': [
                'max',
                lambda x: x.quantile(0.75),
                'median',
                lambda x: x.quantile(0.25),
                'min'
            ]
        }).reset_index()
        
        # カラム名を設定
        summary.columns = ['serial', 'serial_sub', 'max', 'q3', 'median', 'q1', 'min']
        
        # 結果をSQLiteに保存
        output_engine = create_engine(f'sqlite:///{output_db_path}')
        summary.to_sql('summary_table', output_engine, if_exists='replace', index=False)

def compare_results(db1_path, db2_path):
    """2つのサマリーDBの結果を比較する関数"""
    engine1 = create_engine(f'sqlite:///{db1_path}')
    engine2 = create_engine(f'sqlite:///{db2_path}')
    
    # engineからconnectionを取得して使用
    with engine1.connect() as conn1, engine2.connect() as conn2:
        df1 = pd.read_sql_table('summary_table', conn1)
        df2 = pd.read_sql_table('summary_table', conn2)
    
    return pd.testing.assert_frame_equal(df1, df2)

def load_grammar():
    """testlark.larkファイルから文法定義を読み込む"""
    with open('testlark.lark', 'r') as f:
        return f.read()

def load_expression():
    """expr.txtファイルから式を読み込む"""
    with open('expr.txt', 'r') as f:
        return f.read().strip()

from lark import Transformer  # インポートを追加

class SQLTransformer(Transformer):
    def start(self, items):
        return items[0]

    def symbol(self, items):
        v = ValueTable.__table__
        return select(
            v.c.serial,
            v.c.serial_sub,
            v.c.x,
            v.c.y,
            v.c.attr_value.label('calculated_value')
        ).where(v.c.attr_name == str(items[0]))

    def max(self, tree):
        args = tree
        from sqlalchemy import union_all, func
        if isinstance(args, list):
            selects = args
        else:
            selects = [args]
        union = union_all(*selects).alias('unioned')
        return select(
            union.c.serial,
            union.c.serial_sub,
            union.c.x,
            union.c.y,
            func.max(union.c.calculated_value).label('calculated_value')
        ).group_by(union.c.serial, union.c.serial_sub, union.c.x, union.c.y)

    def min(self, tree):
        args = tree
        from sqlalchemy import union_all, func
        if isinstance(args, list):
            selects = args
        else:
            selects = [args]
        union = union_all(*selects).alias('unioned')
        return select(
            union.c.serial,
            union.c.serial_sub,
            union.c.x,
            union.c.y,
            func.min(union.c.calculated_value).label('calculated_value')
        ).group_by(union.c.serial, union.c.serial_sub, union.c.x, union.c.y)

    def mean(self, tree):
        args = tree
        from sqlalchemy import union_all, func
        if isinstance(args, list):
            selects = args
        else:
            selects = [args]
        union = union_all(*selects).alias('unioned')
        return select(
            union.c.serial,
            union.c.serial_sub,
            union.c.x,
            union.c.y,
            func.avg(union.c.calculated_value).label('calculated_value')
        ).group_by(union.c.serial, union.c.serial_sub, union.c.x, union.c.y)

    def median(self, tree):
        args = tree
        from sqlalchemy import union_all, func
        if isinstance(args, list):
            selects = args
        else:
            selects = [args]
        union = union_all(*selects).alias('unioned')
        # SQLiteにはmedian関数がないので、近似値としてavgで代用
        return select(
            union.c.serial,
            union.c.serial_sub,
            union.c.x,
            union.c.y,
            func.avg(union.c.calculated_value).label('calculated_value')
        ).group_by(union.c.serial, union.c.serial_sub, union.c.x, union.c.y)

    def number(self, items):
        from sqlalchemy import literal
        v = ValueTable.__table__
        value = float(items[0])
        return select(
            v.c.serial,
            v.c.serial_sub,
            v.c.x,
            v.c.y,
            literal(value).label('calculated_value')
        ).distinct()

    def arg_list(self, items):
        return [self.transform(item) for item in items]

    def add(self, items):
        left = items[0]
        right = items[1]
        left_alias = left.alias('left')
        right_alias = right.alias('right')
        return select(
            left_alias.c.serial,
            left_alias.c.serial_sub,
            left_alias.c.x,
            left_alias.c.y,
            (left_alias.c.calculated_value + right_alias.c.calculated_value).label('calculated_value')
        ).select_from(
            left_alias.join(
                right_alias,
                (left_alias.c.serial == right_alias.c.serial) &
                (left_alias.c.serial_sub == right_alias.c.serial_sub) &
                (left_alias.c.x == right_alias.c.x) &
                (left_alias.c.y == right_alias.c.y)
            )
        )

    def sub(self, items):
        left = items[0]
        right = items[1]
        left_alias = left.alias('left')
        right_alias = right.alias('right')
        return select(
            left_alias.c.serial,
            left_alias.c.serial_sub,
            left_alias.c.x,
            left_alias.c.y,
            (left_alias.c.calculated_value - right_alias.c.calculated_value).label('calculated_value')
        ).select_from(
            left_alias.join(
                right_alias,
                (left_alias.c.serial == right_alias.c.serial) &
                (left_alias.c.serial_sub == right_alias.c.serial_sub) &
                (left_alias.c.x == right_alias.c.x) &
                (left_alias.c.y == right_alias.c.y)
            )
        )

    def mul(self, items):
        left = items[0]
        right = items[1]
        left_alias = left.alias('left')
        right_alias = right.alias('right')
        return select(
            left_alias.c.serial,
            left_alias.c.serial_sub,
            left_alias.c.x,
            left_alias.c.y,
            (left_alias.c.calculated_value * right_alias.c.calculated_value).label('calculated_value')
        ).select_from(
            left_alias.join(
                right_alias,
                (left_alias.c.serial == right_alias.c.serial) &
                (left_alias.c.serial_sub == right_alias.c.serial_sub) &
                (left_alias.c.x == right_alias.c.x) &
                (left_alias.c.y == right_alias.c.y)
            )
        )

    def div(self, items):
        left = items[0]
        right = items[1]
        left_alias = left.alias('left')
        right_alias = right.alias('right')
        return select(
            left_alias.c.serial,
            left_alias.c.serial_sub,
            left_alias.c.x,
            left_alias.c.y,
            (left_alias.c.calculated_value / right_alias.c.calculated_value).label('calculated_value')
        ).select_from(
            left_alias.join(
                right_alias,
                (left_alias.c.serial == right_alias.c.serial) &
                (left_alias.c.serial_sub == right_alias.c.serial_sub) &
                (left_alias.c.x == right_alias.c.x) &
                (left_alias.c.y == right_alias.c.y)
            )
        )

    def unary_minus(self, items):
        operand = items[0]
        return -operand

    def unary_plus(self, items):
        operand = items[0]
        return operand

    def __default__(self, data, children, meta):
        print(f"Unhandled rule: {data}, children: {children}")
        raise NotImplementedError(f"Rule not handled: {data}")

def main():
    """メイン処理"""
    grammar = load_grammar()
    expr = load_expression()
    parser = Lark(grammar)
    
    # SQLAlchemyによる処理
    query = create_summary_sql(expr, 'dummy_db.sqlite', parser)

    # SQLAlchemyのセッションを使用してクエリを実行
    engine = create_engine('sqlite:///dummy_db.sqlite')
    Base.metadata.create_all(engine)
    
    with Session(engine) as session:
        # カスタム関数を登録
        conn = session.connection().connection
        conn.create_aggregate("percentile_25", 1, Percentile25)
        conn.create_aggregate("percentile_50", 1, Percentile50)
        conn.create_aggregate("percentile_75", 1, Percentile75)
        
        # クエリ実行と結果の保存
        result = session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df)
        df.to_sql('summary_table', engine, if_exists='replace', index=False)
    
    # Pandasによる検証
    # create_summary_pandas(expr, 'dummy_db.sqlite', 'summary_db_test.sqlite')
    
    # 結果の比較
    # compare_results('summary_db.sqlite', 'summary_db_test.sqlite')

if __name__ == '__main__':
    main()

#from impala.sqlalchemy import dialect as impala_dialect

# query = session.query(User).filter(User.name == 'Alice')
# print(str(query.statement.compile(dialect=impala_dialect())))
