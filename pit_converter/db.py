# -*- coding: utf-8 -*-
"""
  db.py
  Author : Jacek 'Szumak' Kotlarski --<szumak@virthost.pl>
  Created: 24.03.2024, 17:32:54
  
  Purpose: 
"""


from inspect import currentframe
from typing import Optional

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    create_engine,
    Text,
)
from sqlalchemy.schema import SchemaConst
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
from sqlalchemy.dialects.sqlite import INTEGER, TEXT
from sqlalchemy.ext.hybrid import hybrid_property

from jsktoolbox.libs.base_data import BData
from jsktoolbox.attribtool import ReadOnlyClass
from jsktoolbox.raisetool import Raise


class _Keys(object, metaclass=ReadOnlyClass):
    """Local keys."""

    DB_PATH = "_db_path_"
    DEBUG = "_debug_"
    DBH = "_db_handler_"


class LocalBase(DeclarativeBase):
    """DeclarativeBase local class."""


# db models
class TNodes(LocalBase):
    """Nodes table."""

    __tablename__: str = "nodes"

    id: Mapped[int] = mapped_column(
        INTEGER, primary_key=True, nullable=False, autoincrement=True
    )
    name: Mapped[str] = mapped_column(TEXT, default="")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id='{self.id}', name='{self.name}')"

    @hybrid_property
    def node_name(self) -> str:
        return f"WW_W{self.id:04}"


class TPoints(LocalBase):
    """Points table."""

    __tablename__: str = "points"

    id: Mapped[int] = mapped_column(
        INTEGER, primary_key=True, nullable=False, autoincrement=True
    )
    name: Mapped[str] = mapped_column(TEXT, default="")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id='{self.id}', name='{self.name}')"

    @hybrid_property
    def point_name(self) -> str:
        return f"P{self.id:04}"


class TWLines(LocalBase):
    """WLines table."""

    __tablename__: str = "wlines"

    id: Mapped[int] = mapped_column(
        INTEGER, primary_key=True, nullable=False, autoincrement=True
    )
    name: Mapped[str] = mapped_column(TEXT, default="")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id='{self.id}', name='{self.name}')"

    @hybrid_property
    def wline_name(self) -> str:
        return f"LB{self.id:04}"


class TServices(LocalBase):
    """Services table."""

    __tablename__: str = "services"

    id: Mapped[int] = mapped_column(
        INTEGER, primary_key=True, nullable=False, autoincrement=True
    )
    name: Mapped[str] = mapped_column(TEXT, default="")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id='{self.id}', name='{self.name}')"

    @hybrid_property
    def service_name(self) -> str:
        return f"PA{self.id:04}"


class Database(BData):
    """Database class engine for local data."""

    def __init__(self, path: str, debug: bool = False) -> None:
        """Constructor."""
        self._data[_Keys.DB_PATH] = path
        self._data[_Keys.DEBUG] = debug
        self._data[_Keys.DBH] = None

        # create engine
        if self.__create_engine():
            base = LocalBase()
            base.metadata.create_all(self._data[_Keys.DBH])

    def __create_engine(self) -> bool:
        """Create Engine for sqlite database."""
        engine: Optional[Engine] = None
        try:
            engine = create_engine(
                f"sqlite:///{self._data[_Keys.DB_PATH]}",
                echo=self._data[_Keys.DEBUG],
            )
        except Exception as ex:
            raise Raise.error(f"{ex}", OSError, self._c_name, currentframe())
        if engine is not None:
            self._data[_Keys.DBH] = engine
            return True
        return False

    @property
    def session(self) -> Optional[Session]:
        """Create Session from Database Engine."""
        session = None
        if self._data[_Keys.DBH] is None:
            return None
        try:
            session = Session(bind=self._data[_Keys.DBH])
        except Exception as ex:
            raise Raise.error(f"{ex}", OSError, self._c_name, currentframe())
        return session


# #[EOF]#######################################################################
