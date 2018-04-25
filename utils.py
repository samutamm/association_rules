
def colums_tranformers(df):
    column_to_id = {}
    id_to_column = {}
    for i, col in enumerate(df.columns):
        column_to_id[col] = i
        id_to_column[i] = col
    return column_to_id,id_to_column