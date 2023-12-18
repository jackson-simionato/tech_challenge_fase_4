import local_env, requests, os
import pandas as pd

api_key = local_env.eia_api_key
api_url = local_env.eia_api_url

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

def get_fob_data(api_url, api_key):
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
    check_dir('./data/eia_fob_price')
    df.to_csv('./data/eia_fob_price/eia_fob_price_EPCBRENT.csv', index=False)

    return df

def get_petroleum_production_data(api_url, api_key):
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
    check_dir('./data/eia_oil_production')
    df.to_csv('./data/eia_oil_production/eia_monthly_oil_production_world.csv', index=False)

    return df

data = get_fob_data(api_url, api_key)
data = get_petroleum_production_data(api_url, api_key)