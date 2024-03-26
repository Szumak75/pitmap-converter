# -*- coding: utf-8 -*-
"""
  conv.py
  Author : Jacek 'Szumak' Kotlarski --<szumak@virthost.pl>
  Created: 24.03.2024, 17:33:12
  
  Purpose: 
"""


import os

from typing import List, Optional

from inspect import currentframe
from jsktoolbox.libs.base_data import BData
from jsktoolbox.attribtool import ReadOnlyClass
from jsktoolbox.libs.system import PathChecker, Env
from jsktoolbox.raisetool import Raise

from pit_converter.db import Database, TNodes, TPoints, TServices, TWLines


class _Keys(object, metaclass=ReadOnlyClass):
    """Keys container class."""

    CACHE_DIR: str = "__cache__"
    DATABASE: str = "__database__"
    DBH: str = "_DBH_"
    COUNTER: str = "_count_"


class Converter(BData):
    """docstring for Converter."""

    def __init__(self) -> None:
        """Constructor."""
        self._data[_Keys.CACHE_DIR] = ".cache/uke-pit"
        self._data[_Keys.DATABASE] = "converter.sqlite"
        self._data[_Keys.COUNTER] = 0

        # init dirs
        self.__init_dirs()

        # init db
        self.__init_db()

    def __init_db(self) -> None:
        """Initialize database connection."""
        tmp: str = os.path.join(
            Env.home, self._data[_Keys.CACHE_DIR], self._data[_Keys.DATABASE]
        )
        db = Database(tmp)
        if db is not None:
            self._db_handler = db
        else:
            raise Raise.error(
                "Init database error.", OSError, self._c_name, currentframe()
            )

    def __init_dirs(self) -> None:
        """Initialize local path for database."""
        tmp: str = os.path.join(
            Env.home, self._data[_Keys.CACHE_DIR], self._data[_Keys.DATABASE]
        )
        # print(tmp)
        pc = PathChecker(tmp)
        if not pc.exists:
            if not pc.create():
                raise Raise.error(
                    f"Cannot create local database: '{tmp}'",
                    OSError,
                    self._c_name,
                    currentframe(),
                )

    @property
    def _db_handler(self) -> Database:
        """The _db_handler property."""
        if _Keys.DBH not in self._data:
            self._data[_Keys.DBH] = None
        return self._data[_Keys.DBH]

    @_db_handler.setter
    def _db_handler(self, value: Database) -> None:
        if not isinstance(value, Database):
            raise Raise.error(
                f"Expected Database type, received '{type(value)}'.",
                TypeError,
                self._c_name,
                currentframe(),
            )
        self._data[_Keys.DBH] = value

    @property
    def __count(self) -> int:
        return self._data[_Keys.COUNTER]

    @__count.setter
    def __count(self, value: int) -> None:
        self._data[_Keys.COUNTER] = value

    def node(self, csv_line: List[str]) -> str:
        session = self._db_handler.session
        if session:
            row: Optional[TNodes] = (
                session.query(TNodes)
                .filter(TNodes.name == f"{csv_line[0]}".upper())
                .first()
            )
            if not row:
                row = TNodes()
                row.name = f"{csv_line[0]}".upper()
                session.add(row)
                session.commit()

            csv_line[0] = row.node_name
            session.close()

        tmp = '","'.join(csv_line)
        return f'"{tmp}"'

    def point(self, csv_line: List[str]) -> str:

        session = self._db_handler.session
        if session:
            # Node: idx==2
            row2: Optional[TNodes] = (
                session.query(TNodes)
                .filter(TNodes.name == f"{csv_line[2]}".upper())
                .first()
            )
            if not row2:
                row2 = TNodes()
                row2.name = f"{csv_line[2]}".upper()
                session.add(row2)
                session.commit()

            csv_line[2] = row2.node_name

            # Point: idx==0
            row0: Optional[TPoints] = (
                session.query(TPoints)
                .filter(TPoints.name == f"{csv_line[0]}".upper())
                .first()
            )
            if not row0:
                row0 = TPoints()
                row0.name = f"{csv_line[0]}".upper()
                session.add(row0)
                session.commit()

            csv_line[0] = row0.point_name

            session.close()

        tmp = '","'.join(csv_line)
        return f'"{tmp}"'

    def service(self, csv_line: List[str]) -> str:

        session = self._db_handler.session
        if session:
            # Point: idx==1
            row1: Optional[TPoints] = (
                session.query(TPoints)
                .filter(TPoints.name == f"{csv_line[1]}".upper())
                .first()
            )
            if not row1:
                row1 = TPoints()
                row1.name = f"{csv_line[1]}".upper()
                session.add(row1)
                session.commit()

            csv_line[1] = row1.point_name

            # Service: idx==0
            # row0: Optional[TServices] = (
            #     session.query(TServices)
            #     .filter(TServices.name == f"{csv_line[0]}".upper())
            #     .first()
            # )
            # if not row0:
            #     row0 = TServices()
            #     row0.name = f"{csv_line[0]}".upper()
            #     session.add(row0)
            #     session.commit()

            # csv_line[0] = row0.service_name

            session.close()
            self.__count += 1
            csv_line[0] = f"AD{self.__count:04}"
            csv_line[2] = ""
        tmp = '","'.join(csv_line)
        return f'"{tmp}"'

    def wline(self, csv_line: List[str]) -> str:

        session = self._db_handler.session
        if session:
            # Node: idx==1
            row1: Optional[TNodes] = (
                session.query(TNodes)
                .filter(TNodes.name == f"{csv_line[1]}".upper())
                .first()
            )
            if not row1:
                row1 = TNodes()
                row1.name = f"{csv_line[1]}".upper()
                session.add(row1)
                session.commit()

            csv_line[1] = row1.node_name

            # Node: idx==2
            row2: Optional[TNodes] = (
                session.query(TNodes)
                .filter(TNodes.name == f"{csv_line[2]}".upper())
                .first()
            )
            if not row2:
                row2 = TNodes()
                row2.name = f"{csv_line[2]}".upper()
                session.add(row2)
                session.commit()

            csv_line[2] = row2.node_name

            # wline: idx==0
            row0: Optional[TWLines] = (
                session.query(TWLines)
                .filter(TWLines.name == f"{csv_line[0]}".upper())
                .first()
            )
            if not row0:
                row0 = TWLines()
                row0.name = f"{csv_line[0]}".upper()
                session.add(row0)
                session.commit()

            csv_line[0] = row0.wline_name

            session.close()

        tmp = '","'.join(csv_line)
        return f'"{tmp}"'


# #[EOF]#######################################################################
