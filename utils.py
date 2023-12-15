import local_env, requests

api_key = local_env.eia_api_key
api_url = local_env.eia_api_url

def get_fob_data(api_url, api_key):
    url = f'{api_url}/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&facets[product][]=EPCBRENT&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=500000'

    response = requests.get(url)
    print(url)

    data = response.json()

    return data

data = get_fob_data(api_url, api_key)