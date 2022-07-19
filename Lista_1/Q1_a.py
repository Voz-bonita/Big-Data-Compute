import requests
from lxml import html
import os

src = "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True"
response = requests.get(src)
doc = html.fromstring(response.text)

# xpath do quadro mínimo que contém todos os links de interesse
xpath = '//*[@id="content"]/div[3]/section/div/div[2]/ul/li'
links = doc.xpath(xpath)[0].findall(".//a")

SAVE_DIR = "./Lista_1/dados/"
if not os.path.exists(SAVE_DIR):
    os.mkdir(SAVE_DIR)

for link in links:
    info = link.text.split()
    uf = info[1]
    parte = "_".join(info[3:])
    print(uf)
    download = requests.get(link.attrib["href"], stream=True)
    with open(f"{SAVE_DIR}{uf}-{parte}.csv", "wb+") as file:
        for chunk in download.iter_content(chunk_size=5 * 1024):
            file.write(chunk)
