import os
import gc
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
                        tables.append({'file': file, 'cur_path': cur_path, 'func': func})
                    break

    db_path = os.path.join(path, 'db.sqlite')
    engine = create_engine('sqlite:///' + db_path)
    db_created = False

    for table in tables:
        if len([val for val in tables if val['file'] == table['file']]) > 1:
            table_name = table['file'] + "".join(table['cur_path'].split(path)[1:])
        else:
            table_name = table['file']
        table_name = '"' + table_name + '"'
        df = table['func'](os.path.join(table['cur_path'], table['file']))
        if not df.empty:
            df.to_sql(table_name, engine)
            db_created = True
    engine.dispose()
    del df
    gc.collect()

    if db_created:
        return db_path
    else:
        return None
