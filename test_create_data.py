import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, date
from sqlalchemy import create_engine, Column, Date, String, Float, MetaData
from create_data import fetch_and_insert_data

class TestCreateData(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:', echo=True)
        self.metadata = MetaData()
        self.stock_data_columns = [
            Column('stock_date', Date),
            Column('stock_code', String(255)),
            Column('closing_price', Float)
        ]
        self.stock_data_table = self.metadata.create_table('stock_data', self.stock_data_columns)
        self.metadata.create_all(self.engine)

    @patch('create_data.si.Price')
    def test_fetch_and_insert_data_existing_row(self, mock_si_price):
        mock_session = MagicMock()
        mock_session.query().filter().first.return_value = True
        data_to_insert = []
        date = datetime(2023, 5, 6).date()

        result = fetch_and_insert_data(date, None, self.stock_data_table, self.engine, mock_session, data_to_insert)

        self.assertEqual(result, 1)
        mock_session.query().filter().first.assert_called_once()
        mock_si_price.assert_not_called()

    @patch('create_data.si.Price')
    def test_fetch_and_insert_data_new_row(self, mock_si_price):
        mock_session = MagicMock()
        mock_session.query().filter().first.return_value = None
        mock_si_price.return_value = [
            {'股價日期': datetime(2023, 5, 6), '證券代號': '0050', '收盤價': 100.0},
            {'股價日期': datetime(2023, 5, 6), '證券代號': '0051', '收盤價': 200.0}
        ]
        data_to_insert = []
        date = datetime(2023, 5, 6).date()

        result = fetch_and_insert_data(date, None, self.stock_data_table, self.engine, mock_session, data_to_insert)

        self.assertEqual(result, 1)
        mock_session.query().filter().first.assert_called_once()
        mock_si_price.assert_called_once_with('20230506', '上市')
        self.assertEqual(len(data_to_insert), 2)
        self.assertEqual(data_to_insert[0], {'stock_date': date(2023, 5, 6), 'stock_code': '0050', 'closing_price': 100.0})
        self.assertEqual(data_to_insert[1], {'stock_date': date(2023, 5, 6), 'stock_code': '0051', 'closing_price': 200.0})
        mock_session.execute.assert_called_once()

    @patch('create_data.si.Price')
    def test_fetch_and_insert_data_no_data(self, mock_si_price):
        mock_session = MagicMock()
        mock_session.query().filter().first.return_value = None
        mock_si_price.return_value = None
        data_to_insert = []
        date = datetime(2023, 5, 6).date()

        result = fetch_and_insert_data(date, None, self.stock_data_table, self.engine, mock_session, data_to_insert)

        self.assertEqual(result, 0)
        mock_session.query().filter().first.assert_called_once()
        mock_si_price.assert_called_once_with('20230506', '上市')
        self.assertEqual(len(data_to_insert), 0)
        mock_session.execute.assert_not_called()

if __name__ == '__main__':
    unittest.main()