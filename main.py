# 注意した点
# ・バリデーション
# ・SQLセキュリティ対策
# ・エラーハンドリング
# ・責務分離
# ・DRY原則
# ・可読性
# -----------------------------------------------------------------------------
# print・inputを含むインタラクティブ処理や、エラー時の終了処理は、本番環境の仕様に合わせて修正が必要です

import csv
import sqlite3
import sys
import os
import logging

# 環境変数の設定
LOG_FILE_PATH = os.environ.get('LOG_FILE_PATH', 'application.log')
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'ERROR'))
LOG_FORMAT = os.environ.get('LOG_FORMAT', '%(asctime)s [%(levelname)s] %(message)s')
DATE_FORMAT= os.environ.get('DATE_FORMAT', '%Y-%m-%d %H:%M:%S')
DB_PATH = os.environ.get('DB_PATH', 'company.db')

# ロギングの設定
logging.basicConfig(filename=LOG_FILE_PATH,
                    level=LOG_LEVEL,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT)

# データベース接続クラス
class DatabaseConnection:
    def __init__(self):
        try:
            self.conn = sqlite3.connect(DB_PATH)
            self.cur = self.conn.cursor()
            self.conn.execute("PRAGMA foreign_keys = ON")  # 外部キー制約を有効にする
        except sqlite3.Error as e:
            logging.error(f'データベースに接続できませんでした: {e}')
            sys.exit(1)

    def __enter__(self):
        return self.conn, self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
            logging.error(f'トランザクション中にエラーが発生しました: {exc_val}')
            print("エラーが発生しました。詳細はログを確認してください。")
        else:
            self.conn.commit()
        self.conn.close()

# （要件外）テーブルの作成
def create_tables(cur):
    try:
        cur.execute('''CREATE TABLE IF NOT EXISTS employees (
                        employee_id INTEGER PRIMARY KEY,
                        employee_name TEXT NOT NULL)''')

        cur.execute('''CREATE TABLE IF NOT EXISTS salaries (
                        employee_id INTEGER,
                        basic_salary INTEGER NOT NULL,
                        FOREIGN KEY(employee_id) REFERENCES employees(employee_id) ON DELETE CASCADE)''')
    except sqlite3.Error as e:
        logging.error(f'テーブルの作成中にエラーが発生しました: {e}')
        raise e

# ファイルとデータのバリデーション
def validate_file_and_data(filename):
    if not os.path.exists(filename):
        message = "ファイルが見つかりません"
        logging.error(message)
        return False

    if not filename.endswith('.csv'):
        message = "CSVファイルを指定してください"
        logging.error(message)
        return False

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        valid_columns = {'社員番号', '社員名', '基本給'}
        fieldnames = set(reader.fieldnames)
        if not fieldnames.issubset(valid_columns):  # 列名が不正でないことを確認
            message = "列名が不正です"
            logging.error(message)
            return False
        if len(fieldnames) != len(reader.fieldnames):  # 列名が重複していないことを確認
            message = "列名が重複しています"
            logging.error(message)
            return False
        for row in reader:  # データに欠損値や不正な値がないことを確認
            employee_id = row.get('社員番号')
            basic_salary = row.get('基本給')
            try:
                if not employee_id or not basic_salary:
                    message = f"欠損値があります: 社員番号 {employee_id}, 基本給 {basic_salary}"
                    logging.error(message)
                    return False
                if int(employee_id) <= 0 or int(basic_salary) <= 0:
                    message = f"正の整数を指定してください: 社員番号 {employee_id}, 基本給 {basic_salary}"
                    logging.error(message)
                    return False
            except ValueError:
                message = f"数値に変換できません: 社員番号 {employee_id}, 基本給 {basic_salary}"
                logging.error(message)
                return False

    return True

# （要件外）未登録社員を各テーブルに登録する
def create_new_employee(cur, employee_id, employee_name):
    try:
        cur.execute("INSERT INTO employees (employee_id, employee_name) VALUES (?, ?)", (employee_id, employee_name))
        cur.execute("INSERT INTO salaries (employee_id, basic_salary) VALUES (?, 0)", (employee_id,))
        print(f"未登録社員を登録しました: 社員番号 {employee_id}, 社員名 {employee_name}")
    except sqlite3.Error as e:
        logging.error(f'未登録社員の登録中にエラーが発生しました: {e}')
        raise e  # エラーを再度スローする

# 未登録社員の登録を確認し実行する
def create_new_employees_from_csv(cur, filename):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        new_employees = []
        for row in reader:
            employee_id = row.get('社員番号')
            employee_name = row.get('社員名', '')
            if employee_id:
                cur.execute("SELECT * FROM employees WHERE employee_id=?", (employee_id,))
                employee = cur.fetchone()
                if not employee:
                    new_employees.append((employee_id, employee_name))
        if new_employees:
            print(f"未登録社員が {len(new_employees)} 件見つかりました")
            for employee in new_employees:
                print(f"社員番号 {employee[0]}, 社員名 {employee[1]}")
            while True:  # ユーザーからの入力をバリデーションする
                answer = input("これらの未登録社員を登録しますか？ (y/n): ").lower()
                if answer in ['y', 'n']:
                    break
                print("無効な入力です。'y'または'n'を入力してください。")
            if answer == 'y':
                for employee in new_employees:
                    create_new_employee(cur, *employee)
                return True
            else:
                return False
        else:
            return True

# 社員番号をキーにしてデータを更新する
def update_data_from_csv(cur, filename, table, column, column_name):
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        data = []
        for row in reader:
            employee_id = row.get('社員番号')
            value = row.get(column_name)
            if employee_id and value:
                try:
                    data.append((value, employee_id))
                except ValueError:
                    print(f"無効な{column}があります: {value}")
        cur.executemany(f"UPDATE {table} SET {column}=? WHERE employee_id=?", data)
        print(f"🎉 合計 {len(data)} 件の{column_name}が更新されました")

# メイン処理
def main():
    if len(sys.argv) != 2:
        print("引数としてCSVファイルを指定してください")
        return

    filename = sys.argv[1]
    if validate_file_and_data(filename):
        with DatabaseConnection() as (conn, cur):
            create_tables(cur)
            if create_new_employees_from_csv(cur, filename):
                update_data_from_csv(cur, filename, 'employees', 'employee_name', '社員名')
                update_data_from_csv(cur, filename, 'salaries', 'basic_salary', '基本給')
            else:
                print("⚡️ 未登録社員が含まれたため、データの更新を中止しました")
    else:
        print("ファイルが不正です。詳細はログを確認してください。")

if __name__ == "__main__":
    main()
