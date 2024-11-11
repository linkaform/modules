import requests

def get_twilio_cred():
    api_key = 'ApiKey paco@linkaform.com:d6545f651c1c1e6615267b5971d3cc2e874dcd25'

    url = 'https://preprod.linkaform.com/api/infosync/user_admin/twilio_creds/'

    headers = {
        'Authorization': f'{api_key}',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code in [200, 201]: 
        data = response.json()
        return data
    else:
        print(f"Error al obtener las credenciales: {response.status_code}")
        print(response.text)
        return None
