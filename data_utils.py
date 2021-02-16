import os
import re
import pandas

from sqlalchemy import create_engine


def store_in_sqlite(path: str, match: re.Pattern = None):
    """
        Scans local path for files in table-like formats
    readable by pandas using default arguments:
    csv, excel, parquet (single file), json (excluded if have nested objects);
        If a file could be read by pandas as nonempty DataFrame
    it is stored in SQLite database as table. Table name
    will be equal to file name. In case there are multiple files
    with the same name then full sub-path is used as table name postfix;
        Database file will be stored at the root of provided path
    named db.sqlite
    :param path: str
        local path to scan including sub-folders
    :param match: re.Pattern object
        files that wouldn't match will be filtered out
    :return: str or None
        full path of resulting database if created
    """
    extensions = {'.csv': 'read_csv',
                  '.xlsx': 'read_excel',
                  '.parquet': 'read_parquet',
                  '.json': 'read_json'}
    tables = []

    for cur_path, cur_dir, files in os.walk(path):
        for file in files:
            for ext in extensions.keys():
                if file.endswith(ext):
                    if match is None or match.fullmatch(file):
                        func = getattr(pandas, extensions[ext])
                        df = func(os.path.join(cur_path, file))
                        if not df.empty:
                            tables.append([file, "".join(cur_path.split(path)[1:]), df])
                    break

    db_path = os.path.join(path, 'db.sqlite')
    engine = create_engine('sqlite:///' + db_path)

    for table in tables:
        if len([val for val in tables if val[0] == table[0]]) > 1:
            table_name = table[0] + table[1]
        else:
            table_name = table[0]
        table_name = '"' + table_name + '"'
        table[2].to_sql(table_name, engine)
    if len(tables) > 0:
        return db_path
    else:
        return None
