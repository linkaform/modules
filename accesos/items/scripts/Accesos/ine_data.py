import base64
import requests

# Define the file path and API details
image_path = "/tmp/ife.jpg"

nufi = {
    'ocr_ine_frente': {'url':"https://nufi.azure-api.net/ocr/v4/frente",'method':'POST'},
    'calcular_rfc': {'url':"https://nufi.azure-api.net/api/v1/calcular_rfc",'method':'POST'},
    'nss': {'url':"https://nufi.azure-api.net/numero_seguridad_social/v2/consultar",'method':'POST'},
}
api_url = 
api_key = "4f146d972ce44b9190380e89bf2242d4"

# Convert the image to Base64
with open(image_path, "rb") as image_file:
    base64_image = base64.b64encode(image_file.read()).decode('utf-8')

# Prepare the JSON payload
payload = {
    "base64_credencial_frente": base64_image
}
print('the payload=', payload)

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

# Output the response
print("Response Status Code:", response.status_code)
print("Response Body:", response.json())