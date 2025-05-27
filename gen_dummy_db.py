import numpy as np
import pandas as pd
import os
from tqdm import tqdm

def generate_dummy_data():
    n_serials = 25  # serial数
    n_positions = 1000  # 各serial_subごとの要素数
    n_attrs = 10000     # attr数

    output_dir = "parq_output"
    os.makedirs(output_dir, exist_ok=True)

    total = n_serials * 25 * n_attrs
    with tqdm(total=total, desc="全体進捗", unit="attr") as pbar:
        for serial in range(n_serials):
            serial_str = f"{serial:09d}"
            for serial_sub in range(1, 26):  # 1から25まで
                serial_sub_str = f"{serial_sub:02d}"

                x_positions = np.random.randint(1, 201, size=n_positions)
                y_positions = np.random.randint(1, 201, size=n_positions)

                data = []
                for attr_idx in range(1, n_attrs + 1):
                    attr_name = f"attr{attr_idx}"
                    attr_values = np.random.normal(0, 1, n_positions)
                    for x, y, attr_value in zip(x_positions, y_positions, attr_values):
                        data.append((serial_str, serial_sub_str, x, y, attr_name, attr_value))
                    pbar.update(1)

                df = pd.DataFrame(data, columns=['serial', 'serial_sub', 'x', 'y', 'attr_name', 'attr_value'])

                # パーティションディレクトリを作成
                partition_dir = os.path.join(output_dir, f"serial={serial_str}", f"serial_sub={serial_sub_str}")
                os.makedirs(partition_dir, exist_ok=True)
                file_path = os.path.join(partition_dir, "data.parquet")

                df.to_parquet(file_path, index=False)

def main():
    print("データ生成開始...")
    generate_dummy_data()
    print("完了しました！")

if __name__ == "__main__":
    main()