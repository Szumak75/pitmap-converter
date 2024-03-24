#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
  conv_nodes.py
  Author : Jacek 'Szumak' Kotlarski --<szumak@virthost.pl>
  Created: 24.03.2024, 15:38:12
  
  Purpose: 
"""


import sys
import csv
from typing import List

from pit_converter.conv import Converter

if __name__ == "__main__":

    data: List[str] = sys.stdin.readlines()

    if data:
        col_header: str = data.pop(0)
        print(col_header, end="")
        conv = Converter()

        for line in csv.reader(data):
            print(conv.point(line))

    sys.exit(0)

# #[EOF]#######################################################################
