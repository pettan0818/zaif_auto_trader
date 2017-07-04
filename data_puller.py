# -*- coding: utf-8 -*-
# !/usr/bin/env python
# vim: set fileencoding=utf-8 :

"""
#
# Author:   Noname
# URL:      https://github.com/pettan0818
# License:  MIT License
# Created: 金  6/30 15:25:12 2017

# Usage
#
"""
import datetime
import sys
import time

import pandas
import sqlalchemy
from zaifapi import impl as zaif

DB_ADDRESS = None
DB_TABLE = "zaif_ticker"
CURRENCY_PAIR = "btc_jpy"  # We talk about btc and jpy exchange.
ZAIF_TRADE_DATA_KEY = "trades"
# INVALID_DATACOL = ["currency_pair"]


def arrange_data(pulled_data: dict) -> pandas.DataFrame:
    """Arrange pulled data to neat dataframe."""
    converted_df = pandas.DataFrame.from_dict(pulled_data)

    converted_df.date = converted_df.date.apply(datetime.datetime.fromtimestamp)

    # del converted_df[INVALID_DATACOL]

    return converted_df


class DataPullExecuter():
    """Pull zaif data stream and make sqldatabase."""
    def __init__(self):
        """Setup Executer.

        * Fetch latest tid from SQL.
        * Connect with SQL.
        """
        self.db_con = sqlalchemy.create_engine(DB_ADDRESS)
        self.latest_tid = None
        if self.latest_tid is None:
            # SQLから最新tidをフェッチする。
            try:
                self.latest_tid = self.db_con.execute("""SELECT MAX(tid) FROM {}""".format(DB_TABLE)).fetchall()[0][0]
            except sqlalchemy.exc.OperationalError:  # データベースがからなどの場合、latest_tidは0にすると、差分うまくとれる。
                self.latest_tid = 0

    def execute(self):
        """Main Process of executer.

        * Pull data from zaif.
        * Make data to SQL.
        """
        stream_pub_api = zaif.ZaifPublicStreamApi()
        try:
            while True:
                # TODO: 後でAsync実装する。
                # streamAPIでデータを引いてくる。
                zaif_newest_data = next(stream_pub_api.execute(CURRENCY_PAIR))  # Type: dict
                # APIから出てきたデータの形式整理
                trading_newest_data = zaif_newest_data[ZAIF_TRADE_DATA_KEY]
                trading_newest_data_df = arrange_data(trading_newest_data)
                # 形式整理をしたデータから差分を取り出す。
                diff_df = trading_newest_data_df[trading_newest_data_df.tid > self.latest_tid]
                # tidが一意なので最新のtidだけ持っておく。
                self.latest_tid = trading_newest_data_df.tid.max()
                # 差分をSQLに突っ込む
                diff_df.to_sql(DB_TABLE, self.db_con, if_exists="append")
                # print(diff_df)
                if diff_df.shape[0] == 0:
                    time.sleep(2)

        except zaif.ZaifApiError:
            pass

    def recover(self):
        """recovery data method."""
        pass


def main(db_name="./tester.db"):
    """Pull zaif data stream and make sqldatabase.
    Usage:
    python ./data_puller.py
    で自動的に処理開始します。"""
    # Initialize
    global DB_ADDRESS
    DB_ADDRESS = "sqlite:///{}".format(db_name)

    executer = DataPullExecuter()
    executer.execute()

    sys.exit(2)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

    main()
