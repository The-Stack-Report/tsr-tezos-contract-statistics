import math

def replace_NaN_in_dict(source_dict, replace_with=False):
    for key in source_dict.keys():
        if isinstance(source_dict[key], float) and math.isnan(source_dict[key]):
            # print(f"{key} = float with value {source_dict[key]}")
            source_dict[key] = replace_with
            # print(f"Replaced: {key}")
        elif isinstance(source_dict[key], dict):
            replace_NaN_in_dict(source_dict[key], replace_with=replace_with)
        elif type(source_dict[key]) is list:
            for val in source_dict[key]:
                if isinstance(val, float) and math.isnan(val):
                    # print(f"Replaced val in: {key}")
                    val = replace_with
    
    return source_dict