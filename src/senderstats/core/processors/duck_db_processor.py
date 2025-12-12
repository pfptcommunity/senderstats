from datetime import datetime
from typing import Dict, Any, Optional

import duckdb

from senderstats.data.message_data import MessageData
from senderstats.interfaces.processor import Processor


class DuckDBProcessor(Processor[MessageData]):
    def __init__(self, db_file: str, chunk_size: int = 100000):
        super().__init__()
        self.con = duckdb.connect(db_file)
        self.table_name = 'sender_data'
        self.columns: Optional[list[str]] = None
        self.type_map = {
            str: 'VARCHAR',
            int: 'INTEGER',
            float: 'DOUBLE',
            list: 'ARRAY[VARCHAR]',
            datetime: 'BIGINT',
            dict: 'JSON',
        }
        self.chunk: list[tuple] = []
        self.chunk_size = chunk_size

        # Counters if making Reportable
        self._insert_count: int = 0

    def _infer_schema(self, data: MessageData) -> None:
        attrs: Dict[str, Any] = vars(data)
        self.columns = sorted(attrs.keys())
        column_defs = []
        for col in self.columns:
            val = attrs[col]
            py_type = type(val) if val is not None else str  # Default for None
            sql_type = self.type_map.get(py_type, 'VARCHAR')
            column_defs.append(f"{col} {sql_type}")
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({', '.join(column_defs)})"
        self.con.execute(create_sql)
        print(f"Inferred schema for DuckDB: {create_sql}")  # Debug log

    def _build_tuple(self, data: MessageData) -> tuple:
        attrs = vars(data)
        tuple_vals = []
        for col in self.columns:
            val = attrs.get(col)
            if val is None:
                tuple_vals.append(None)
            elif isinstance(val, datetime):
                tuple_vals.append(int(val.timestamp()))
            elif type(val) not in self.type_map:
                tuple_vals.append(str(val))
            else:
                tuple_vals.append(val)
        return tuple(tuple_vals)

    def execute(self, data: MessageData) -> None:
        if self.columns is None:
            self._infer_schema(data)

        self.chunk.append(self._build_tuple(data))
        self._insert_count += 1

        if len(self.chunk) >= self.chunk_size:
            self._flush()

    def _flush(self) -> None:
        if self.chunk and self.columns:
            placeholders = ','.join(['?'] * len(self.columns))
            insert_sql = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
            self.con.executemany(insert_sql, self.chunk)
            self.chunk = []

    def finalize(self) -> None:
        self._flush()
        self.con.close()
        print(f"Finalized DuckDB insert: {self._insert_count} records processed.")  # Debug
