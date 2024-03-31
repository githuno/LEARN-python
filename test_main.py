import sqlite3
import unittest
from unittest.mock import patch, mock_open
import main  # テスト対象のモジュール

class TestMain(unittest.TestCase):
    def setUp(self):
        # テスト前のセットアップ
        self.conn = sqlite3.connect(':memory:')  # メモリ上の一時データベースを作成
        self.cur = self.conn.cursor()
        self.cur.execute('''CREATE TABLE employees (
                            employee_id INTEGER PRIMARY KEY,
                            employee_name TEXT NOT NULL)''')
        self.cur.execute('''CREATE TABLE salaries (
                            employee_id INTEGER PRIMARY KEY,
                            basic_salary INTEGER NOT NULL)''')

    def tearDown(self):
        # テスト後のクリーンアップ
        self.conn.close()

    @patch('sqlite3.connect')
    def test_update_salary(self, mock_connect):
        # テスト前のセットアップ
        mock_connect.return_value = self.conn
        # 社員マスタと給与マスタにテストデータを挿入
        self.cur.execute("INSERT INTO employees (employee_id, employee_name) VALUES (1, 'Test Employee')")
        self.cur.execute("INSERT INTO salaries (employee_id, basic_salary) VALUES (1, 1000)")

        # 給与の更新をテスト
        updated = main.update_salary(1, 2000)
        self.assertEqual(updated, 1)
        self.cur.execute("SELECT basic_salary FROM salaries WHERE employee_id=1")
        self.assertEqual(self.cur.fetchone()[0], 2000)

if __name__ == '__main__':
    unittest.main()
