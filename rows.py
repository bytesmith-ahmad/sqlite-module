from sqlite3 import Row

class Rows: # wrapper for sqlite list of rows
    """Wrapper for sqlite3 Rows"""
    def __init__(self, rows: list[Row], table: str = None):
        self.table = table
        self.columns = self.get_columns(rows)
        self.rows = self.convert_to_dict(rows)

    # @staticmethod #todo
    def get_columns(rows = None): return rows[0].keys() # get keys from first row in list
    
    # @staticmethod #todo
    def get_rows(): raise NotImplementedError
    
    # @staticmethod
    def convert_to_dict(rows):
        raise NotImplementedError()

    def get_values(self) -> list[list]:
        l = []
        for row in self.rows:
            l += [[val for val in row]]
        return l
