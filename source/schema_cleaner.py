import re
import os

import pandas as pd


def header_column_cleaner(col_list):
    cleaned_columns_list = []
    try:
        # removing all special chars from column names and creating column list
        pattern = r'[:()\-?/\\.$%]'
        for col in col_list:
            cleaned_columns_list.append('_'.join(list(map(lambda _col: str(_col).capitalize() ,re.sub(pattern, ' ', col).split()))))

        return cleaned_columns_list

    except Exception as e:
        print("Error in header_column_cleaner", e)


def schema_cleaner(field_mapping_file, sheet):
    try:
        if os.path.isfile(field_mapping_file):
            _df = pd.read_excel(field_mapping_file, sheet_name=sheet)
            _clean_col_names = header_column_cleaner(list(_df['Field_Name'].values))

            # handling repeating column names
            _repeat_col_names = {}

            # getting count for each repeating column name
            for col in _clean_col_names:
                if _clean_col_names.count(col) > 1:
                    _repeat_col_names.update({col: _clean_col_names.count(col)})

            # adjusting repeat column names with suffix 1,2,3, ...
            _adjusted_repeat_column_names = {}
            for col, count in _repeat_col_names.items():
                _adjusted_repeat_column_names[col] = [col + '_' + str(i) for i in range(1, count + 1)]

            # updating column list for repeat columns
            for col in _clean_col_names:
                if col in _adjusted_repeat_column_names.keys():
                    if len(_adjusted_repeat_column_names[col]) > 0:
                        _clean_col_names[_clean_col_names.index(col)] = _adjusted_repeat_column_names[col][0]
                        _adjusted_repeat_column_names[col].remove(_adjusted_repeat_column_names[col][0])

            # creating col names dict for future use
            _clean_col_names_dict = { col : str for col in _clean_col_names }

            # getting field positions
            _field_start_position = ''.join(list(_df['Field_Length'].values))


            # getting conversion type
            _df['Size'] = _df['Size'].astype(str)
            _conversion = zip(list(_df['Conversion'].values), list(_df['Size'].values))
            _conversion_dict = {}
            for col_nm, convert_type in zip(_clean_col_names, _conversion):
                _conversion_dict[col_nm] = convert_type

            return _clean_col_names, _clean_col_names_dict, _field_start_position, _conversion_dict
        else:
            raise Exception('Field mapping excel file not found')

    except Exception as e:
        print("Error in schema_cleaner :", e)



if __name__ =="__main__":
    print(schema_cleaner("/home/vm_user/Navitus_EST_Pipeline/pipe_config/field_mappings_v0.xlsx", "Details")[3])
