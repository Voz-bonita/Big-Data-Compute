import requests
import zipfile, io
import os


def download(link, path):
    """
    Faz o download dos arquivos zip  já salvando
    os resultados descompatados em uma pasta e
    criando a mesma caso ainda não exista
    """
    r = requests.get(link)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    if not os.path.exists(path):
        os.mkdir(path)
    z.extractall(path)


estados = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]
anos = range(1996, 2021)

src = "https://datasus.saude.gov.br/wp-content/download.php"
db = "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES"
payload = {}


for estado in estados:
    print(estado)
    for ano in anos:
        print(ano)
        file = f"DN{estado}{ano}.dbc"
        payload = {
            "dados[0][arquivo]": file,
            "dados[0][link]": f"{db}/{file}",
        }
        response = requests.post(src, payload)

        zip_dbc = response.json()[0][0].replace("//", "/").replace("/", "//")

        download(zip_dbc, path="./Lista_3/datasus")
