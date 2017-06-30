# -*- coding: utf-8 -*-
# !/usr/bin/env python
# vim: set fileencoding=utf-8 :

"""
#
# Author:   Noname
# URL:      https://github.com/pettan0818
# License:  MIT License
# Created: 金  6/30 14:25:41 2017

# Usage
#
"""
from zaifapi import impl
CURRENCY_PAIR = "btc_jpy"

PUB_API = impl.ZaifPublicApi()
stream_pub_api = impl.zaifpublicstreamapi()

current_price = PUB_API.last_price(CURRENCY_PAIR)

while True:
    test = STREAM_PUB_API.execute(CURRENCY_PAIR)
    data = next(test)
    print(data['timestamp'])
    print(data['last_price'])
    print(data['trades'])

if __name__ == '__main__':
    import doctest
    doctest.testmod()
