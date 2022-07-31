import requests

IBGE = "https://sage.saude.gov.br/paineis/regiaoSaude/lista.php?output=jsonbt&&order=asc&_=1659303484372"
response = requests.get(IBGE)
mapa_codigos = response.json()

with open("./Lista_1/mapa_codigos.csv", "w+") as file:
    file.writelines("estabelecimento_municipio_codigo, code_health_region\n")
    for registro in mapa_codigos:
        file.writelines(f'{registro["ibge"]}, {registro["co_colegiado"]}\n')
