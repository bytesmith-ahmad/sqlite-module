# Code by Ahmad Al-Jabbouri

from sqlite3 import Error, Row, Connection, connect, PARSE_DECLTYPES
import adapters, converters
from .rows import Rows
from logging import info, exception
from pypika import Query, Table, Field
from pathlib import PureWindowsPath as Path

class SQLite():
    """
    Use pyPika to build and pass Query to execute
    """

    _dir = Path(__file__).parent
    #TODO ADD ITERATOR __iter__

    def __init__(self, db_path: Path = None):
        self.connection: Connection = self.connect(db_path) if db_path else None #! bad: returns string, put conditional below instead
        self.tables: list[Table] = self.get_tables() if self.connection else None #! bad

    def connect(self,db_path: Path) -> str: #! NOT CONNECTION
        """Connects to database and configures connection"""
        
        info(f"Connecting to {db_path}...")
        try:
            self.connection = connect(
                database=db_path,
                detect_types= PARSE_DECLTYPES  # To use in conjuction to converters and adapters
            )
            self.connection.row_factory = Row # .execute() will now return Rows instead of tuples. Rows work similar to dict
            for value in self.connection.execute("SELECT sqlite_version()").fetchone():
                return value
        except Exception as e:
            exception(f"Failed connection: {e}")
            
    def commit(self) -> None:
        self.connection.commit()
    
    def close(self) -> None:
        self.connection.close()
        
    def get_tables(self) -> list[str]:
        rows = self.connection.execute(
            """SELECT name FROM sqlite_master WHERE
            type = 'table' AND name NOT LIKE 'sqlite%';"""
        ).fetchall()
        return [Table(v) for row in rows for v in row]
    
    def get_fields(self,T:Table) -> list[str]:
        rows = self.connection.execute(
            f"SELECT name FROM pragma_table_info({str(T)})"
        ).fetchall()
        return [T.field(v) for row in rows for v in row]

    def execute(self,sql: Query, rowid = True) -> Report:
        try:
            table = self.get_table_name(sql)
            #! THIS v WILL INTRODUCES BACKSLASHES IF QUERY CONTAINS SINGLE-QUOTES
            sql = str(sql).replace('*','rowid AS id, *') if rowid else str(sql)
            rows = self.connection.execute(sql).fetchall()
            if rows == []:
                rows = self.connection.execute(f"SELECT rowid AS id, * FROM {table}").fetchall()
            r = self.Report(table=table,rows=rows)
        except Error as sql_e:
            r = self.Report(error=sql_e)
        except Exception as ex:
            r = self.Report(error=ex)
        finally:
            return r

    @staticmethod
    def get_table_name(sql:Query) -> str:
        """Extracts table name from query, doesn't not work for all queries"""
        if bool(sql._from):
            """SELECT""" # for future use
            t = sql._from[0].get_table_name()
        elif bool(sql._insert_table):
            """INSERT"""
            t = sql._insert_table.get_table_name()
        elif bool(sql._update_table):
            """UPDATE"""
            t = sql._update_table.get_table_name()
        else:
            """unknown"""
            t = "\033[31mNOT FOUND\033[0m"
        return t
    
    # TODO
    def select(db,t,columns = '*',rowid=True) -> DataStore.Report:
        q = t.select('rowid', *columns) # * is for unpacking
        rep = db.execute(q)
        return rep

    # TODO
    def insert(db,t):
        data = [
            Otolith('sdf','sdfs','sdfsd','sdfs',3454,65,88), #! SINGLE QUOTES BREAK PROGRAM
            Otolith('fgh','fgh','fgh','gh',566,45,645),
            Otolith('fg','jh','df','jnhb',5996,34,86)
        ]
        q = t.insert(*[o.as_values() for o in data])
        rep = db.execute(q)
        return rep

    # TODO
    def update(db:DataStore,t,field):
        q = t.update().set('number', 55).where(field == 2)
        db.execute(q)
        pass

    # TODO
    def delete(d):
        pass

    #^ TODO
    def map_to_otolith(rows) -> list[Otolith]:
        otos = []
        for row in rows:
            otos += [Otolith(**row)]
        return otos[0]

    # TODO Begin tests

    system('cls')

    # D > T > C > R
    D = connect(db_name)     #* success
    T = D.get_tables()       #* success
    C = D.get_fields(T[0])   #* success

    @staticmethod
    def toCSV(data,fname="output.csv"):
        with open(fname,'a') as file:
            file.write(",".join([str(j) for i in data for j in i]))
            
    @staticmethod
    def fromCSV():
        raise NotImplementedError()

    @staticmethod
    def summary(rows):
            
        # split the rows into columns
        cols = [ [r[c] for r in rows] for c in range(len(rows[0])) ]
        
        # the time in terms of fractions of hours of how long ago
        # the sample was assumes the sampling period is 10 minutes
        t = lambda col: "{:.1f}".format((len(rows) - col) / 6.0)

        # return a tuple, consisting of tuples of the maximum,
        # the minimum and the average for each column and their
        # respective time (how long ago, in fractions of hours)
        # average has no time, of course
        ret = []

        for c in cols:
            hi = max(c)
            hi_t = t(c.index(hi))

            lo = min(c)
            lo_t = t(c.index(lo))

            avg = sum(c)/len(rows)

            ret.append(((hi,hi_t),(lo,lo_t),avg))

        return ret

if __name__ == "__main__":
    # TODO TEST CODE HERE
    parent = f"{__file__}\\..\\test.py"
