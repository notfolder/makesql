## テストDB仕様

ある要素について、複数の属性値がある場合の縦持ちテーブル。
ある要素はserialとserial_subでまとまっており、その中でx,yの位置で特定される。
serial,serial_sub,x,yである要素が特定される。
serialはunique(DB上は制約をつけない)
serial_subは1~25までの数字。
x,yは1~200の範囲の数字
attr_nameは基本的には全ての要素が持つ属性の名前

- value_table

|フィールド名|形|概要|
|----|----|----|
|serial|str|9桁0パディングの数字|
|serial_sub|str|2桁0パディングの数字|
|x|int|要素のxの位置|
|y|int|要素のyの位置|
|attr_name|str|属性名|
|attr_value|float|属性値|

この条件でpythonでnumpy,pandasを利用してダミーデータを作成し、
sqliteファイルにデータを出力したい。

要素はserialを100程度。serial_subはそれぞれのserialについて25個生成。
それぞれのserialとserial_subの組み合わせに対して、要素を1000個作成。
それぞれの要素に対して、下記の属性とそれに対するランダムな属性値を生成する

|属性名|値の生成方法|
|----|----|
|attr1|1σの正規分布|
|attr2|1σの正規分布|
|attr3|(attr1+attr2)/2+0.5σの正規分布値|

このダミーデータを作成してデータ出力をするプログラム(gen_dummy_db.py)を作成して。
sqliteファイルはdummy_db.sqliteで。
