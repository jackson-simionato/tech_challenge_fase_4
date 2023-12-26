import local_env, requests, os
import pandas as pd

api_key = local_env.eia_api_key
api_url = local_env.eia_api_url

project_id = 'brave-tea-400210'
dataset_id = 'eia_oil_data'

def check_dir(path):
    """
    This function checks if a directory exists at the specified path, and creates it if it doesn't.

    Parameters:
    path (str): The path to the directory to be checked.

    Returns:
    None
    """
    if not os.path.exists(path):
        return os.makedirs(path)

def get_fob_data(api_url, api_key, project_id, dataset_id):
    # URL da requisição
    url = f'{api_url}/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&facets[product][]=EPCBRENT&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000'
    response = requests.get(url)

    # Filtro dos dados
    data = response.json()['response']['data']

    # Criação do DF
    df = pd.DataFrame.from_dict(data)

    c = 1

    while len(data) >= 5000:
        # URL da requisição
        url = f'{api_url}/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&facets[product][]=EPCBRENT&sort[0][column]=period&sort[0][direction]=desc&offset={c*5000}&length=5000'
        response = requests.get(url)

        # Filtro dos dados
        data = response.json()['response']['data']

        # Criação do DF do request
        request_df = pd.DataFrame.from_dict(data)

        # Concatenação com DF final
        df = pd.concat([df, request_df], axis=0)

        c += 1

    df.reset_index(drop=True, inplace=True)

    # Exportação
    fname = 'eia_fob_price_EPCBRENT.csv'

    check_dir('data/eia_fob_price')

    df.to_csv(f'data/eia_fob_price/{fname}', index=False)

    # Upload para Google Big Query
    table_id = fname.split('.')[0]

    # Use the pandas_gbq.to_gbq() function to insert the DataFrame into BigQuery
    pd.io.gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

    return df

def get_petroleum_production_data(api_url, api_key, project_id, dataset_id):
    # URL da requisição
    url = f'{api_url}/international/data/?api_key={api_key}&frequency=monthly&data[0]=value&facets[activityId][]=1&facets[productId][]=53&facets[countryRegionId][]=WORL&facets[unit][]=TBPD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000'
    response = requests.get(url)

    # Filtro dos dados
    data = response.json()['response']['data']

    # Criação do DF
    df = pd.DataFrame.from_dict(data)

    c = 1

    while len(data) >= 5000:
        # URL da requisição
        url = f'{api_url}/international/data/?api_key={api_key}&frequency=monthly&data[0]=value&facets[activityId][]=1&facets[productId][]=53&facets[countryRegionId][]=WORL&facets[unit][]=TBPD&sort[0][column]=period&sort[0][direction]=desc&offset={c*5000}&length=5000'
        response = requests.get(url)

        # Filtro dos dados
        data = response.json()['response']['data']

        # Criação do DF do request
        request_df = pd.DataFrame.from_dict(data)

        # Concatenação com DF final
        df = pd.concat([df, request_df], axis=0)

        c += 1

    df.reset_index(drop=True, inplace=True)

    # Exportação
    fname = 'eia_monthly_oil_production_world.csv'

    check_dir('./data/eia_oil_production')
    
    df.to_csv(f'./data/eia_oil_production/{fname}', index=False)

    # Upload para Google Big Query
    table_id = fname.split('.')[0]

    # Use the pandas_gbq.to_gbq() function to insert the DataFrame into BigQuery
    pd.io.gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

    return df

def get_energy_consumption_data(api_url, api_key, project_id, dataset_id):
    # URL da requisição
    url = f'{api_url}/international/data/?api_key={api_key}&frequency=annual&data[0]=value&facets[activityId][]=2&facets[productId][]=44&facets[countryRegionId][]=WORL&facets[unit][]=QBTU&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000'
    response = requests.get(url)

    # Filtro dos dados
    data = response.json()['response']['data']

    # Criação do DF
    df = pd.DataFrame.from_dict(data)

    c = 1

    while len(data) >= 5000:
        # URL da requisição
        url = f'{api_url}/international/data/?api_key={api_key}&frequency=annual&data[0]=value&facets[activityId][]=2&facets[productId][]=44&facets[countryRegionId][]=WORL&facets[unit][]=QBTU&sort[0][column]=period&sort[0][direction]=desc&offset={c*5000}&length=5000'
        response = requests.get(url)

        # Filtro dos dados
        data = response.json()['response']['data']

        # Criação do DF do request
        request_df = pd.DataFrame.from_dict(data)

        # Concatenação com DF final
        df = pd.concat([df, request_df], axis=0)

        c += 1

    df.reset_index(drop=True, inplace=True)

    # Exportação
    fname = 'eia_annual_energy_consumption_world.csv'

    check_dir('./data/eia_energy_consumption')

    df.to_csv(f'./data/eia_energy_consumption/{fname}', index=False)

    # Upload para Google Big Query
    table_id = fname.split('.')[0]

    # Use the pandas_gbq.to_gbq() function to insert the DataFrame into BigQuery
    pd.io.gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

    return df

def get_oil_consumption_data(api_url, api_key, project_id, dataset_id):
    # URL da requisição
    url = f'{api_url}/international/data/?api_key={api_key}&frequency=annual&data[0]=value&facets[activityId][]=2&facets[productId][]=5&facets[countryRegionId][]=WORL&facets[unit][]=TBPD&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000'
    response = requests.get(url)

    # Filtro dos dados
    data = response.json()['response']['data']

    # Criação do DF
    df = pd.DataFrame.from_dict(data)

    c = 1

    while len(data) >= 5000:
        # URL da requisição
        url = f'{api_url}/international/data/?api_key={api_key}&frequency=annual&data[0]=value&facets[activityId][]=2&facets[productId][]=5&facets[countryRegionId][]=WORL&facets[unit][]=TBPD&sort[0][column]=period&sort[0][direction]=desc&offset={c*5000}&length=5000'
        response = requests.get(url)

        # Filtro dos dados
        data = response.json()['response']['data']

        # Criação do DF do request
        request_df = pd.DataFrame.from_dict(data)

        # Concatenação com DF final
        df = pd.concat([df, request_df], axis=0)

        c += 1

    df = df.loc[df['value'] != '--']
    df['value'] = df['value'].astype(float)
    df.drop(columns=['dataFlagId','dataFlagDescription'], inplace=True)

    df.reset_index(drop=True, inplace=True)

    # Exportação
    fname = 'eia_annual_oil_consumption_world.csv'

    check_dir('./data/eia_oil_consumption')

    df.to_csv(f'./data/eia_oil_consumption/{fname}', index=False)

    # Upload para Google Big Query
    table_id = fname.split('.')[0]

    print(df.dtypes)

    # Use the pandas_gbq.to_gbq() function to insert the DataFrame into BigQuery
    pd.io.gbq.to_gbq(df, f'{project_id}.{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')

    return df

def get_eia_data(api_url, api_key, project_id, dataset_id):

    get_fob_data(api_url, api_key, project_id, dataset_id)
    get_petroleum_production_data(api_url, api_key, project_id, dataset_id)
    get_energy_consumption_data(api_url, api_key, project_id, dataset_id)
    get_oil_consumption_data(api_url, api_key, project_id, dataset_id)

    return print('Aquisição de dados finalizada com sucesso!')

#get_eia_data(api_url, api_key, project_id, dataset_id)
get_oil_consumption_data(api_url, api_key, project_id, dataset_id)