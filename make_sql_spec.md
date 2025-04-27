db_spec.mdで作成したDBに対して操作するpythonプログラム(make_sql.py)を作りたい。

SQLAlchemyでCDataを使ってdummy_db.sqliteを読み込んで下記の処理をしたい。

作りたいプログラムの大きな目的は汎用的にtestlark.larkで定義された式(expr.txt)について、
dummy_db.sqlite,serial_subの組み合わせ毎にboxplotに相当するmax,q3,median,q1,minを計算、
下記構造を持ったsqliteテーブルに出力するsql文をSQLAlchemyで作成する関数を作りたい。
この関数ではexpr.txtに指定された式はtestlark.larkの文法に従う限り可変であることを前提とする。

- dummy_db.sqliteのvalue_table

|フィールド名|形|概要|
|----|----|----|
|serial|str|9桁0パディングの数字|
|serial_sub|str|2桁0パディングの数字|
|x|int|要素のxの位置|
|y|int|要素のyの位置|
|attr_name|str|属性名|
|attr_value|float|属性値|


- summary_db.sqlite

|フィールド名|形|概要|
|----|----|----|
|serial|str|9桁0パディングの数字|
|serial_sub|str|2桁0パディングの数字|
|sum_type|str|max,q3,median,q1,min|
|attr_name|str|属性名|
|attr_value|float|集計値|

また、上記関数に対してテストするために、pandasでなるべく簡易に作成した同様の計算をしてsummary_db_test.sqliteファイルを作成する関数を作成し、
summary_db.sqliteの出力とsummary_db_test.sqliteの出力を比較してテストできる様にしたい。
このテスト関数は汎用的でなくて良いため、expr.txtの内容は固定されている前提で作成して良い。

