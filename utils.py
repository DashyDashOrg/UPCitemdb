import pandas as pd
import re
import numpy as np

# Normalise unit measures
MEASURES = {"oz":list(sorted(["oz","ounce", "ounces", "oz."], key = len, reverse=True)),
        "fl oz":list(sorted(["fluid ounce","fluid oz", "fl. oz", 'fl.oz', 'fl.oz.', 'floz'], key = len, reverse=True)),
        "lb":list(sorted(["pound","lb","lbs", "pounds", 'lbs.'], key = len, reverse=True)),
        "inches":list(sorted(["in","inch","inches",'"'], key = len, reverse=True)),
        "ft":list(sorted(["ft","feet","feets"], key = len, reverse=True)),
        "gal":list(sorted(["gal", "gallon", 'ga', 'gal.'], key = len, reverse=True)),
        "gr":list(sorted(["gram", "grm", "gr", 'gm'], key = len, reverse=True)),
        "grn":list(sorted(["grain", "grn"], key = len, reverse=True)),
        "dz":list(sorted(["dz", "dozen"], key = len, reverse=True)),
        "liter":list(sorted(["lit", "lt", "liter", 'l', 'ltr'], key = len, reverse=True)),
        'yard':list(sorted(['yard', "yd", "yds", 'yards'], key = len, reverse=True)),
        "kg":list(sorted(["kilogram", "kgs", 'kilograms', 'kg'], key = len, reverse=True)),
        "ml":list(sorted(["ml"], key = len, reverse=True))
}

# Rename webscraped columns to normalised ones
def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    column_mapping = {
        "title": "name",
        "Product_Image": "item_image",
        "Product_Price": "price",
        'Product_URL': 'product_url',
        "category": "detailed_category",
        "Category_2": "sub_category",
        "Category_3": "detail_category",
        "Category1": "category",
        "Category2": "sub_category",
        "Category3": "detail_category",
        "Unit_Amount_Type": "unit_measure",
        "Unit_Amount": "unit_size",
        "Unit_Quantity": "pack_count",
        "Brand": 'brand',
        "updated_t": 'extracted_time',
        'upc': 'UPC',
        'gtin': 'GTIN',
        'ean': 'EAN',
        'asin': 'ASIN'
    }
    df = df.rename(
        columns=column_mapping
    )
    return df

def extract_image(df: pd.DataFrame) -> pd.DataFrame:
    def foo(x):
        if "['" in x:
            x = x.split("['")[1]
            x = x.split(',')[0]
            x = x[:-1]
            return x
        else:
            return ''
    df['item_image'] = df['images'].apply(foo)
    return df
def extract_name(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_brand(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_price(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_category(df: pd.DataFrame) -> pd.DataFrame:
    def foo1(x):
        cats = x.split('>')
        cats = [c.strip() for c in cats]
        if len(cats) >= 1:
            return cats[0]
        return ''
    def foo2(x):
        cats = x.split('>')
        cats = [c.strip() for c in cats]
        if len(cats) >= 2:
            return cats[1]
        return ''
    def foo3(x):
        cats = x.split('>')
        cats = [c.strip() for c in cats]
        if len(cats) >= 3:
            return cats[2]
        return ''
    
    df['category'] = df['detailed_category'].apply(foo1)
    df['sub_category'] = df['detailed_category'].apply(foo2)
    df['detail_category'] = df['detailed_category'].apply(foo3)
    return df

def extract_unit_size(df: pd.DataFrame) -> pd.DataFrame:
    regex_string = []
    for key,val in MEASURES.items():
        regex_string.append(key)
        for v in val:
            regex_string.append(v)
    regex_string = list(set(regex_string))
    look_ahead = '|'.join(regex_string)
    #regex_string = '(\d+(?:\.\d*)?\\s?(?=({})))'.format(look_ahead)
    #regex_string = '(\d+(?:\.\d*)?\\s?(?=({})(?:\\s|$|,)))'.format(look_ahead)
    #regex_string = '(\d*\.?\d+?\\s?(?=({})(?:\\s|$|,)))'.format(look_ahead)
    regex_string = '((?:^|\s)\d*\.?\d+?\\s?(?=({})(?:\\s|$|,)))'.format(look_ahead)

    def foo_size(x):
        x = x.lower()
        x = x.replace('-', ' ')
        x = x.replace('  ', ' ')
        x = x.replace(',', '')
        x = x.replace('(', ' ')
        x = x.replace(')', ' ')
        res = re.findall(regex_string, x)

        if len(res) > 0:
            return res[0][0].strip()
        return ''
    def foo_measure(x):
        x = x.lower()
        x = x.replace('-', ' ')
        x = x.replace('  ', ' ')
        x = x.replace(',', '')
        x = x.replace('(', ' ')
        x = x.replace(')', ' ')
        res = re.findall(regex_string, x)
        if len(res) > 0:
            return res[0][1].strip()
        return ''

    df['unit_size'] = df['name'].apply(foo_size)
    df['unit_measure'] = df['name'].apply(foo_measure)
    df = normalise_unit_measure(df)

    """ # Extracts numeric value: '4.5 LB' -> '4.5'
    # '[\d]+[.,\d]+|[\d]*[.][\d]+|[\d]+'
    unit_size = df['Unit_Amount_and_Unit_Amount_Type'].\
        apply(lambda x: "" if pd.isnull(x) else re.findall(r"[ \t]*(?:\d|[^\w\s])+[ \t]*", x))
    # If we get multiple values, we take last one
    df['unit_size'] = unit_size.apply(lambda x: '' if len(x) == 0 else x[-1].strip())
    # Some of them have erroneous '#'. Replace those
    df['unit_size'] = df['unit_size'].str.replace('#', '') """

    # Validate unit_size
    try:
        df['unit_size'].apply(lambda x: True if x == '' else float(x))
    except Exception as e:
        print('Problem with unit_size column')
        print(str(e))

    return df

def normalise_unit_measure(df: pd.DataFrame) -> pd.DataFrame:
    df['unit_measure'] = df['unit_measure'].fillna('')
    df['unit_measure'] = df['unit_measure'].str.lower()
    df['unit_measure'] = df['unit_measure'].str.strip()
    #df['unit_measure'] = df['unit_measure'].str.replace('. ', ' ', regex=False) 

    lookup_measures = {}
    for key,val in MEASURES.items():
        for v in val:
            lookup_measures[v] = key
    df['unit_measure'] = df['unit_measure'].apply(lambda x: x if x not in lookup_measures else lookup_measures[x])
    return df
            
def extract_unit_measure(df: pd.DataFrame) -> pd.DataFrame:
    # Validate unit_measure column
    _check = df['unit_measure'].apply(lambda x: False if x =='' else any(i.isdigit() for i in x))
    if sum(_check) >= 1:
        print('Problem with unit_measure column')
    return df

def extract_pack_count(df: pd.DataFrame) -> pd.DataFrame:
    def f(x):
        x = x.lower()
        x = x.replace('-', ' ')
        x = x.replace('  ', ' ')
        x = x.replace(',', '')
        x = x.replace('#', '')
        x = x.replace('(', ' ')
        x = x.replace(')', ' ')
        try:
            if 'pack of' in x:
                x = x.split('pack of')[1]
                x = re.findall('\d*\.?\d+?', x)[0]
            elif ' pack' in x:
                x = x.split(' pack')[0]
                x = re.findall('\d*\.?\d+?', x)[-1]
            elif 'case of' in x:
                x = x.split('case of')[1]
                x = re.findall('\d*\.?\d+?', x)[0]
            elif 'per case' in x:
                x = x.split('per case')[0]
                x = re.findall('\d*\.?\d+?', x)[-1]
            elif 'case' in x:
                x = x.split('case')[0]
                x = re.findall('\d*\.?\d+?', x)[-1]
            pack_count = int(x)
        except:
            pack_count = ''
        return pack_count
        
    def foo(x):
        x = x.lower()
        if 'pack of' in x:
            x = x.split('pack of')
            try:
                return int(re.findall('\d*\.?\d+?', x[1])[0])
            except:
                ''
        elif 'pack' in x:
            x = x.split('pack')
            try:
                return int(re.findall('\d*\.?\d+?', x[0])[0])
            except:
                ''
        return ''
    #df['pack_count'] = df['size'].apply(foo)
    df['pack_count'] = df['name'].apply(f)

    # Validate pack_count
    try:
        df['pack_count'].apply(lambda x: True if x == '' else float(x))
    except Exception as e:
        print('Problem with pack_count column')
        print(str(e))
    return df

def calculate_price_per_unit(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_SKU(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_GTIN(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_UPC(df: pd.DataFrame) -> pd.DataFrame:
    return df

def extract_MPN(df: pd.DataFrame) -> pd.DataFrame:
    return df

def set_needed_columns(df: pd.DataFrame) -> pd.DataFrame:
    needed_columns = [
        'dataset',
        'hash',
        'product_url',
        'name',
        'supplier',
        'brand',
        'price',
        'unit_size',
        'unit_measure',
        'pack_count',
        'price_per_unit',
        'category',
        'sub_category',
        'detail_category',
        'SKU',
        'GTIN',
        'UPC',
        'MPN',
        'ASIN',
        'item_image',
        'image_description',
        'extracted_time'
    ]
    df[list(set(needed_columns) - set(df.columns))] = ""
    df = df[needed_columns].copy()
    return df

def set_dataset(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    df['dataset'] = dataset
    return df

def set_supplier(df: pd.DataFrame, supplier: str) -> pd.DataFrame:
    df['supplier'] = df['is_offer'].apply(lambda x: supplier + '-offer' if x == '1' else supplier)
    return df

def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(keep="first")

    # Replace the nulls with empty string
    df = df.fillna("")
    df = extract_image(df)
    df = rename_cols(df)
    df = extract_name(df)
    df = extract_brand(df)
    df = extract_price(df)
    df = extract_category(df)
    df = extract_unit_size(df)
    #df = extract_unit_measure(df)
    df = extract_pack_count(df)
    df = calculate_price_per_unit(df)
    df = extract_SKU(df)
    df = extract_GTIN(df)
    df = extract_UPC(df)
    df = extract_MPN(df)
    df = set_supplier(df, 'UPCitemdb')
    df = set_needed_columns(df)
    df = set_dataset(df, 'API')
    return df


f  = pd.read_csv('files/enriched.csv', dtype="str")
f = normalise(f)
f.to_csv('Normalised UPCitemdb 2022-03-31.csv', index=False)
#terraform apply -target=module.webscrape_normalise_lambda -var-file="variables.tfvars"
#terraform plan -target=module.webscrape_normalise_lambda -var-file="variables.tfvars"