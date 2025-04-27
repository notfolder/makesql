import numpy as np
import pandas as pd
import sqlite3
from tqdm import tqdm

# データベース接続
def create_connection():
    conn = sqlite3.connect('dummy_db.sqlite')
    return conn

# テーブル作成
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS value_table (
        serial TEXT,
        serial_sub TEXT,
        x INTEGER,
        y INTEGER,
        attr_name TEXT,
        attr_value REAL
    )
    ''')
    conn.commit()

def generate_dummy_data():
    # パラメータ設定
    n_serials = 100  # serial数
    n_positions = 1000  # 各serial_subごとの要素数
    
    all_data = []
    
    # プログレスバー付きでデータ生成
    for serial in tqdm(range(n_serials), desc="Generating data"):
        serial_str = f"{serial:09d}"
        
        for serial_sub in range(1, 26):  # 1から25まで
            serial_sub_str = f"{serial_sub:02d}"
            
            # x, y座標の生成
            x_positions = np.random.randint(1, 201, size=n_positions)
            y_positions = np.random.randint(1, 201, size=n_positions)
            
            # 属性値の生成
            attr1_values = np.random.normal(0, 1, n_positions)
            attr2_values = np.random.normal(0, 1, n_positions)
            attr3_values = (attr1_values + attr2_values) / 2 + np.random.normal(0, 0.5, n_positions)
            
            # データフレーム用のデータ作成
            for x, y, attr1, attr2, attr3 in zip(x_positions, y_positions, 
                                               attr1_values, attr2_values, attr3_values):
                all_data.extend([
                    (serial_str, serial_sub_str, x, y, 'attr1', attr1),
                    (serial_str, serial_sub_str, x, y, 'attr2', attr2),
                    (serial_str, serial_sub_str, x, y, 'attr3', attr3)
                ])
    
    # データフレームの作成
    df = pd.DataFrame(all_data, columns=['serial', 'serial_sub', 'x', 'y', 'attr_name', 'attr_value'])
    return df

def main():
    print("データ生成開始...")
    df = generate_dummy_data()
    
    print("SQLiteデータベースに書き込み中...")
    conn = create_connection()
    create_table(conn)
    
    # データベースに書き込み
    df.to_sql('value_table', conn, if_exists='replace', index=False)
    
    conn.close()
    print("完了しました！")

if __name__ == "__main__":
    main()