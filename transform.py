import pandas as pd
import glob, io, requests

def Get_Latest_Version_From_CMD():
    '''
    Pulls the latest v4 from CMD for cpih
    '''
    editions_url = 'https://api.beta.ons.gov.uk/v1/datasets/cpih01/editions/time-series/versions'
    items = requests.get(editions_url + '?limit=1000').json()['items'] 

    # get latest version number
    latest_version_number = items[0]['version']
    assert latest_version_number == len(items), 'Number of versions does not match latest version number'
    # get latest version URL
    url = editions_url + "/" + str(latest_version_number)
    # get latest version data
    latest_version = requests.get(url).json()
    # decode data frame
    file_location = requests.get(latest_version['downloads']['csv']['href'])
    file_object = io.StringIO(file_location.content.decode('utf-8'))
    df = pd.read_csv(file_object, dtype=str)
    return df

def Transform():  
    files = glob.glob('*.csv')

    input_file = [file for file in files if "wda" in file.lower()][0]
    output_file = 'v4-cpih.csv'

    source = pd.read_csv(input_file, dtype=str)

    source.columns = [col.strip() for col in source.columns] # removing whitespace
    source.columns = ['v4_0', 'timeunit', 'time', 'uk_only', 'geography', 'cpih1dim1', 'aggregate']
    
    source['time_codelist'] = source['time']

    # reordering columns
    source = source[[
            'v4_0', 'time_codelist', 'time', 'uk_only', 'geography', 'cpih1dim1', 'aggregate'
            ]]

    # renaming columns
    source = source.rename(columns={
            'time_codelist':'mmm-yy',
            'time':'Time',
            'uk_only':'uk-only',
            'geography':'Geography',
            'aggregatre':'Aggregate',
            'aggregate':'Aggregate',
            'cpih1dim1':'cpih1dim1aggid'
            }
        )

    # read in previous v4 file - which is same as output file
    #previous_v4 = Get_Latest_Version_From_CMD()
    previous_v4 = pd.read_csv(output_file, dtype=str) # output_file is also previous v4

    # combine the two
    df = pd.concat([previous_v4, source])
    
    # 2 incorrect codes that need removing
    df = df[df['cpih1dim1aggid'] != 'cpih1dim1S40900']
    df = df[df['cpih1dim1aggid'] != 'cpih1dim1S40200']
    
    # turns all obs into str of a float - so duplicates can be correctly picked up
    df['v4_0'] = df['v4_0'].apply(lambda x: str(float(x)))
    df = df.drop_duplicates()
    
    df.to_csv(output_file, index=False)

    print("Transform complete")