from . import tables


def get_table_refs():
    tables_dict = {}
    for table_name in tables.__all__:
        TableClass = getattr(tables, table_name)
        tables_dict[TableClass().title] = TableClass
    return tables_dict
