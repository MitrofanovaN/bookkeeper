"""
Модуль описывает репозиторий, использующий БД
"""

from typing import Any
import sqlite3
from inspect import get_annotations
from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQLiteRepository(AbstractRepository[T]):
    """
    Репозиторий, взаимодействующий с таблицей БД
    Атрибуты
    db_file: str - название файла с БД
    cls: type - модель, с объектами котрой работает репозиторий
    """

    db_file: str
    cls: type
    table_name: str
    fields: dict[str, Any]

    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.cls = cls
        self.table_name = self.cls.__name__
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')

        keys = [str(k) for k in self.fields.keys()]
        vals = [str(v) for v in self.fields.values()]
        vals = ['INTEGER' if v.find('int') != -1 else 'TEXT' for v in vals]
        names = [str(k) + ' ' + str(v) for (k, v) in zip(keys, vals)]
        names = ['pk INTEGER PRIMARY KEY'] + names
        names = ', '.join(names)
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {self.table_name}')
            cur.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name} ({names})')
        con.close()

    def add(self, obj: T) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled `pk` attribute')
        names = ', '.join(self.fields.keys())
        placeholders = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(
                f'INSERT INTO {self.table_name} ({names}) VALUES ({placeholders})', values
            )
            obj.pk = cur.lastrowid
        con.close()
        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'SELECT * FROM {self.table_name} WHERE pk = {pk}'
            )
            tuple_obj = cur.fetchone()
        con.close()
        if tuple_obj is None:
            return None
        obj = self.cls(*tuple_obj)
        return obj

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'SELECT * FROM {self.table_name}'
            )
            tuple_objs = cur.fetchall()
        con.close()
        objs = []
        for tuple_obj in tuple_objs:
            objs.append(self.cls(*tuple_obj))
        if where is None:
            return objs
        objs = [obj for obj in objs if
                all(getattr(obj, attr) == where[attr] for attr in where.keys())]
        return objs

    def update(self, obj: T) -> None:
        if obj.pk == 0:
            raise ValueError('attempt to update object with unknown primary key')
        names = list(self.fields.keys())
        sets = ', '.join(f'{name} = \'{getattr(obj, name)}\'' for name in names)
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'UPDATE {self.table_name} SET {sets} WHERE pk = {obj.pk}'
            )
        con.close()

    def delete(self, pk: int) -> None:
        if self.get(pk) is None:
            raise KeyError
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(
                f'DELETE FROM {self.table_name} WHERE pk = {pk}'
            )
        con.close()
