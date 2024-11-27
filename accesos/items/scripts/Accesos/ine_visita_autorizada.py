import base64
import requests
from pprint import pprint

# Define the file path and API details
image_path = "ine.png"

nufi = {
    'ocr_ine_frente': {'url':"https://nufi.azure-api.net/ocr/v4/frente",'method':'POST'},
    'calcular_rfc': {'url':"https://nufi.azure-api.net/api/v1/calcular_rfc",'method':'POST'},
    'nss': {'url':"https://nufi.azure-api.net/numero_seguridad_social/v2/consultar",'method':'POST'},
    'obtener_nss': {'url':"https://nufi.azure-api.net/numero_seguridad_social/v2/status",'method':'POST'},
}
api_url = nufi['ocr_ine_frente']['url']
api_key = ""

# Convert the image to Base64
with open(image_path, "rb") as image_file:
    base64_image = base64.b64encode(image_file.read()).decode('utf-8')

# Prepare the JSON payload
payload = {
    "base64_credencial_frente": base64_image
}
#print('the payload=', payload)

# Define headers for the request
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "NUFI-API-KEY": api_key
}

# Make the POST request
print('sending the post')
print('sending the url=', api_url)
print('sending the headers=', headers)
response = requests.post(nufi['ocr_ine_frente']['url'], json=payload, headers=headers)

rfc_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Ocp-Apim-Subscription-Key": api_key
}

#Obtener datos de la ine
data = response.json()
nombres = data.get('data', {}).get('ocr', {}).get('nombre')
apellido_paterno = data.get('data', {}).get('ocr', {}).get('apellido_paterno')
apellido_materno = data.get('data', {}).get('ocr', {}).get('apellido_materno')
fecha_nacimiento = data.get('data', {}).get('ocr', {}).get('fecha_nacimiento')
curp = data.get('data', {}).get('ocr', {}).get('curp')

#Obtener RFC
rfc_payload = {
    "nombres": nombres,
    "apellido_paterno": apellido_paterno,
    "apellido_materno": apellido_materno,
    "fecha_nacimiento": fecha_nacimiento
}
rfc_response = requests.post(nufi['calcular_rfc']['url'], json=rfc_payload, headers=rfc_headers)

#Generar uuid para luego obtener nss
nss_headers = {
    "NUFi-API-KEY": "",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "NUFI-API-KEY": api_key
}

nss_payload = {
    "curp": curp,
    # "webhook": "https://webhook.site/ed114c1f-566d-4a84-a110-b39a77ca447e",
}
nss_response = requests.post(nufi['nss']['url'], json=nss_payload, headers=nss_headers)

#Obtener uuid de el NSS
data = nss_response.json()
uuid_nss = data.get('data', {}).get('uuid', {})

info_nss_payload = {
    "uuid_nss": uuid_nss,
    "uuid_historial": "string"
}
info_nss_headers = {
    "NUFI-API-KEY": api_key,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

info_nss_response = requests.post(nufi['obtener_nss']['url'], json=info_nss_payload, headers=info_nss_headers)

# Output the response RFC
print("RFC Calculation Response Status Code:", rfc_response.status_code)
print("RFC Calculation Response Body:", rfc_response.json())

# Output the response INE
print("Response Status Code:", response.status_code)
print("Response Body:", response.json())

# Output the response NSS
print("RFC Calculation Response Status Code:", nss_response.status_code)
print("RFC Calculation Response Body:", nss_response.json())

###########################################
print('INE/////////////////////')
pprint(response.json())
print('RFC/////////////////////')
pprint(rfc_response.json())
print('UUID NSS/////////////////////')
pprint(nss_response.json())
print('INFO NSS/////////////////////')
pprint(info_nss_response.json())