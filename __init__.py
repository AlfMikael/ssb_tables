# coding: utf-8

#ssb
#A conventient way to retrieve DataFrames from a selected set of tables from the Norwegian Bureau of Statistics

import pandas as pd
import requests
import ast
from pyjstat import pyjstat
from collections import OrderedDict


def get_variables(
        table_id=None,
        source=None,
        language='en',
        base_url='http://data.ssb.no/api/v0',
        full_url=None):


    if full_url is None:
        full_url = '{base_url}/{language}/table/{table_id}'.format(
            base_url=base_url, language=language, table_id=table_id)

    df = pd.read_json(full_url)
    variables = [dict(values) for values in df.iloc[:, 1]]

    return variables

def read_with_json(table_id=None,
                   query=None,
                   language='en',
                   base_url='http://data.ssb.no/api/v0',
                   full_url=None):

    if full_url is None:
        full_url = '{base_url}/{language}/table/{table_id}'.format(
            base_url=base_url,
            language=language,
            table_id=table_id)

    data = requests.post(full_url, json=query)
    results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict))
    return results[0]

def full_json(table_id=None,
              out='dict',
              language='en',
              full_url=None):


    variables = get_variables(table_id, language=language, full_url=full_url)
    nvars = len(variables)
    var_list = list(range(nvars))

    query_element = {}

    for x in var_list:
        query_element[x] = '{{"code": "{code}", "selection": {{"filter": "item", "values": {values} }}}}'.format(
            code=variables[x]['code'],
            values=variables[x]['values'])
        query_element[x] = query_element[x].replace("\'", '"')
    all_elements = str(list(query_element.values()))
    all_elements = all_elements.replace("\'", "")

    query = '{{"query": {all_elements} , "response": {{"format": "json-stat" }}}}'.format(all_elements=all_elements)

    if out == 'dict':
        query = ast.literal_eval(query)

    return query



def read_all(table_id=None,
             language='en',
             base_url='http://data.ssb.no/api/v0',
             full_url=None):


    if full_url is None:
        full_url = '{base_url}/{language}/table/{table_id}'.format(
            base_url=base_url,
            language=language,
            table_id=table_id)

    query = full_json(full_url=full_url)
    data = requests.post(full_url, json=query)
    results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict))

    # maybe this need not be its own function,
    # but an option in read_json? json = 'all'

    # other functions(options include: read_recent to get only the
    # most recent values (defined as x), json = 'recent')

    return results[0]


def search(phrase,
           language='en',
           base_url='http://data.ssb.no/api/v0'):
    """
        Search for tables that contain the phrase in Statistics Norway.
        Returns a pandas dataframe with the results.

        Example
        -------

            df = search("income")


        Parameters
        ----------

        phrase: string
            The phrase can contain several words (space separated):
                search("export Norwegian parrot")

            It also supports trucation:
                search("pharma*")

            Not case sensitive.
            Language sensitive (specified in the language option)

        language: string
            default in Statistics Norway: 'en' (Search for English words)
            optional in Statistics Norway: 'no' (Search for Norwegian words)

        url: string
            default in Statistics Norway: 'http://data.ssb.no/api/v0'
            different defaults can be specified

        """

    # todo: make converter part of the default specification only for statistics norway
    convert = {'æ': '%C3%A6', 'Æ': '%C3%86', 'ø': '%C3%B8', 'Ø': '%C3%98',
               'å': '%C3%A5', 'Å': '%C3%85',
               '"': '%22', '(': '%28', ')': '%29', ' ': '%20'}

    search_str = '{base_url}/{language}/table/?query={phrase}'.format(
        base_url=base_url,
        language=language,
        phrase=phrase)

    for k, v in convert.items():
        search_str = search_str.replace(k, v)

    # print(search_str)

    df = pd.read_json(search_str)

    if len(df) == 0:
        print("No match")
        return df

    # make the dataframe more readable
    # (is it worth it? increases vulnerability. formats may differ and change)
    # todo: make search and format conditional on the database being searched

    # split the table name into table id and table text

    df['table_id'] = df['title'].str.split(':').str.get(0)
    df['table_title'] = df['title'].str.split(':').str.get(1)
    del df['title']

    # make table_id the index, visually more intuitive with id as first column
    df = df.set_index('table_id')

    # change order of columns to make it more intuitive (table_title is first)
    cols = df.columns.tolist()
    cols.sort(reverse=True)
    df = df[cols[:-2]]

    return df


#Tables 07161 - 10794 concern national tests
def get_frame_from_07161():
    #Variabler - se her for å definere datasett nedenfor

    # Reading the whole dataset
    df = read_all("07161")

    # Fixing column names
    df.columns = ["region", "grade", "test", "level", "sex", "education", "students", "year", "percent"]

    # Deleting 'contents' because it's a superfluous field
    del df["students"]

    #Changing the 5nd, 8nd, 9nd grade, to the more appropriate 5th, 8th, 9th-grade
    df.grade = df.grade.str.replace('nd', 'th')
    df.sex = df.sex.str.replace("Females", "Girls").str.replace("Males", "Boys")

    return df
def get_frame_from_07167():
    #Variabler - se her for å definere datasett nedenfor

    # Reading the whole dataset
    df = read_all("07167")

    #Fixing column names
    df.columns = ["test", "level", "pupils", "contents", "year", "percent"]

    #Deleting 'contents' because it's a superfluous field
    del df["contents"]

    return df
def get_frame_from_07168():
    #Variabler - se her for å definere datasett nedenfor


    #Reading the whole dataset
    df = read_all("07168")

    #Fixing column-names
    df.columns = ["grade", "test", "level", "centrality", "education", "contents", "year", "percent"]

    #Deleting 'contents' because it's a superfluous field
    del df["contents"]

    # Changing the 5nd, 8nd, 9nd grade, to the more appropriate 5th, 8th, 9th-grade
    df.grade = df.grade.str.replace('nd', 'th')

    return df
def get_frame_from_07170():

    df = read_all("07170")

    columns = ["grade", "test", "level", "background", "education", "contents", "year", "percent"]
    df.columns = columns

    #Deleting 'contents' because it's a superfluous field
    del df["contents"]

    # Changing the 5nd, 8nd, 9nd grade, to the more appropriate 5th, 8th, 9th-grade
    df.grade = df.grade.str.replace('nd', 'th')

    return df
def get_frame_from_08558():

    #Reading the whole dataset
    df = read_all("08558")

    #Fixing column-names
    df.columns = ["region", "test_8th", "level_8th", "test_5th", "level_5th", "education", "contents",
               "year", "percent"]

    #Deleting 'contents' because it's a superfluous field
    del df["contents"]

    return df
def get_frame_from_09818():
    #Variabler - se her for å definere datasett nedenfor

    # Reading the whole dataset-
    df = read_all("09818")

    # Fixing column names
    df.columns = ["grade", "test", "level", "immigrant", "education", "contents", "year", "percent"]

    #Deleting 'contents' because it's a superfluous field
    del df["contents"]

    # Changing the 5nd, 8nd, 9nd grade, to the more appropriate 5th, 8th, 9th-grade
    df.grade = df.grade.str.replace('nd', 'th')

    return df
def get_frame_from_10793():
    #Variables - how to define a data-set.

    #Reading all data
    df = read_all("10793")

    #Fixing column names
    df.columns = ["grade", "test", 'background', "sex", "contents", "year", "value"]

    #Fixing typos in dataset
    df.grade = df.grade.str.replace("nd", "th")

    #Restructuring
    #The reason this is done is because the values for score and number of pupils
    #is inexplicably in the same column.
    df_score = df.rename(columns={'value': 'score'})
    df_score = df_score[df_score.contents == 'Score points']
    df_score.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils (persons)']
    df_pupils.reset_index(drop=True, inplace=True)

    return pd.concat([df_score, df_pupils.pupils], axis=1).drop(columns=['contents'])
    #Variables - how to define a data-set.


    #Reading all data
    df = read_all("10793")

    #Fixing column names
    df.columns = ["grade", "test", 'background', "sex", "contents", "year", "value"]

    #Fixing typos in dataset
    df.grade = df.grade.str.replace("nd", "th")

    #Restructuring
    #The reason this is done is because the values for score and number of pupils
    #is inexplicably in the same column.
    df_score = df.rename(columns={'value': 'score'})
    df_score = df_score[df_score.contents == 'Score points']
    df_score.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils (persons)']
    df_pupils.reset_index(drop=True, inplace=True)

    return pd.concat([df_score, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_10794():
    #Variables - how to define a data-set.

    df = read_all("10794")
    df.grade = df.grade.str.replace("nd", "th")
    df.columns = ["region", "grade", "test", "sex", "education", "contents", "year", "value"]

    df_score = df.rename(columns={'value': 'score'})
    df_score = df_score[df_score.contents == 'Score points']
    df_score.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils (persons)']
    df_pupils.reset_index(drop=True, inplace=True)

    return pd.concat([df_score, df_pupils.pupils], axis=1).drop(columns=['contents'])

#Tables 07495 - 11690 concern pupil grades
def get_frame_from_07495():
    #Marks, lower secondary school
    #07495: Lower secondary school points, by sex and parents' educational attainment level (C) 2009 - 2018

    #Reading all data
    df = read_all("07495")
    print("running local folder")
    #Fixing column names
    df.columns = ["region", "sex", 'education', "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'points'})
    df_points = df_points[df_points.contents == 'Average lower secondary school points']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07496():
    #Marks, lower secondary school
    #07496: Overall achievement marks, by subject, sex and parents' educational attainment level (C) 2009 - 2018

    #Reading all data
    df = read_all("07496")
    print("running local folder")
    #Fixing column names
    df.columns = ["region", "subject", 'sex', "education", "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'points'})
    df_points = df_points[df_points.contents == 'Average overall achievement mark']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07497():
    #Marks, lower secondary school
    #07497: Lower secondary school marks, by immigration category and sex 2009 - 2018

    #Reading all data
    df = read_all("07497")
    #Fixing column names
    df.columns = ["background", "sex", 'contents', "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'points'})
    df_points = df_points[df_points.contents == 'Average lower secondary school points']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07498():
    #Marks, lower secondary school
    #07498: Examination marks, by subject, sex and parents' educational attainment level 2009 - 2018

    #Reading all data
    df = read_all("07498")
    #Fixing column names
    df.columns = ["subject", "sex", 'education', "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'marks'})
    df_points = df_points[df_points.contents == 'Average examination mark']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07499():
    #Marks, lower secondary school
    #07499: Overall achievement marks, by subject, immigration category and sex 2009 - 2018

    #Reading all data
    df = read_all("07499")
    #Fixing column names
    df.columns = ["subject", "background", 'sex', "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_marks = df.rename(columns={'value': 'marks'})
    df_marks = df_marks[df_marks.contents == 'Average overall achievement mark']
    df_marks.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_marks, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07500():
    #Marks, lower secondary school
    #07500: Examination marks, by subject, immigration category and sex 2009 - 2018

    #Reading all data
    df = read_all("07500")
    #Fixing column names
    df.columns = ["subject", "background", 'sex', "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'marks'})
    df_points = df_points[df_points.contents == 'Average examination mark']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07501():
    #Marks, lower secondary school
    #07501: Examination and overall achievement marks, selected subjects, by ownership and parents' educational attainment level 2009 - 2018

    #Reading all data
    df = read_all("07501")
    #Fixing column names
    df.columns = ["subject", "ownership", 'education', "contents", "year", "value"]

    #Restructuring - because points and number of students are in the same column

    df_points = df.rename(columns={'value': 'marks'})
    df_points = df_points[df_points.contents == 'Average mark']
    df_points.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_points, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_07502():
    #Marks, lower secondary school
    #07502: Distribution of pupils by overall achievement marks, by subject, sex and parents' educational attainment level 2009 - 2018

    #Reading all data
    df = read_all("07502")
    #Fixing column names
    df.columns = ["marks", "subject", 'sex', "education", "contents", "year", "value"]

    #Restructuring - because percent and number of students are in the same column

    df_percent = df.rename(columns={'value': 'percent'})
    df_percent = df_percent[df_percent.contents == 'Pupils (per cent)']
    df_percent.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)


    return pd.concat([df_percent, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_08533():
    #Marks, lower secondary school
    #08533: Distribution of pupils, by overall achievement marks, subject, test and mastering level on national tests in 8th grade 2010 - 2018
    #Reading all data
    df = read_all("08533")
    #Fixing column names
    df.columns = ["marks", "subject", 'test', "education", "contents", "year", "percent"]


    return df.drop(columns=['contents'])
def get_frame_from_11688():
    #Marks, lower secondary school
    #11688: Pupils, by sex and lower secondary school points (C) 2015 - 2018
    #Reading all data
    df = read_all("11688")
    #Fixing column names
    df.columns = ["region", "sex", 'points', "contents", "year", "value"]

    #Restructuring - because percent and number of students are in the same column

    df_percent = df.rename(columns={'value': 'percent'})
    df_percent = df_percent[df_percent.contents == 'Pupils (per cent)']
    df_percent.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)
    return pd.concat([df_percent, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_11689():
    #Marks, lower secondary school
    #11689: Pupils, by sex, secondary school points and parents' educational attainment level 2015 - 2018
    #Reading all data
    df = read_all("11689")
    #Fixing column names
    df.columns = ["sex", "points", 'education', "contents", "year", "value"]

    #Restructuring - because percent and number of students are in the same column

    df_percent = df.rename(columns={'value': 'percent'})
    df_percent = df_percent[df_percent.contents == 'Pupils (per cent)']
    df_percent.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)
    return pd.concat([df_percent, df_pupils.pupils], axis=1).drop(columns=['contents'])
def get_frame_from_11690():
    #Marks, lower secondary school
    #11690: Pupils, by sex, immigration category and lower secondary school points 2015 - 2018
    #Reading all data
    df = read_all("11690")
    #Fixing column names
    df.columns = ["sex", "background", 'points', "contents", "year", "value"]

    #Restructuring - because percent and number of students are in the same column

    df_percent = df.rename(columns={'value': 'percent'})
    df_percent = df_percent[df_percent.contents == 'Pupils (per cent)']
    df_percent.reset_index(drop=True, inplace=True)

    df_pupils = df.rename(columns={'value': 'pupils'})
    df_pupils = df_pupils[df_pupils.contents == 'Pupils']
    df_pupils.reset_index(drop=True, inplace=True)
    return pd.concat([df_percent, df_pupils.pupils], axis=1).drop(columns=['contents'])


TABLE_DICT = {"07161": get_frame_from_07161, "07167": get_frame_from_07167,
                  "07168": get_frame_from_07168, "07170": get_frame_from_07170,
                  "08558": get_frame_from_08558, "09818": get_frame_from_09818,
                  "07495": get_frame_from_07495, "07496": get_frame_from_07496,
                  "07497": get_frame_from_07497, "07498": get_frame_from_07498,
                  "07499": get_frame_from_07499, "07500": get_frame_from_07500,
                  "07501": get_frame_from_07501, "07502": get_frame_from_07502,
                  "08533": get_frame_from_08533, "11688": get_frame_from_11688,
                  "11689": get_frame_from_11689, "11690": get_frame_from_11690,
                  "10793": get_frame_from_10793, "10794": get_frame_from_10794
                  }


def create_all_tables(folder="tables/"):
    table_names = get_table_codes()

    for table in table_names:
        df = get_table(table).astype(str)
        df.to_csv(f"{folder}table_{table}.csv", index=False)
        print(f"Downloaded and created table_{table}")
    print("Finished creating tables")
    create_title_file(folder)


def create_title_file(folder="tables/"):
    df = get_table_titles().astype(str)
    df.to_csv(f"{folder}titles.csv", index=False)
    print("Finished creating title fil e")


def get_table(table):
    # Also sorts the table
    df = TABLE_DICT[table]()
    return pd.DataFrame({x: df[x].sort_values().values for x in df.columns.values})


def get_table_codes():
    return TABLE_DICT.keys()

def get_table_titles():
    df = pd.DataFrame(columns=('table_code', 'title'))

    for table_code in TABLE_DICT:
        res = search(table_code)
        title = res.iloc[0, 0]
        df = df.append({'table_code': table_code, 'title': title}, ignore_index=True)

    return df
