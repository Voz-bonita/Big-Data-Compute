import requests
import json


def main():
    IBGE = "https://sage.saude.gov.br/paineis/regiaoSaude/lista.php?output=jsonbt&&order=asc&_=1659303484372"
    response = requests.get(IBGE)
    mapa_codigos = response.json()

    dict_codigos = {
        registro["ibge"]: registro["co_colegiado"] for registro in mapa_codigos
    }

    with open("./Lista_1/mapa_codigos.json", "w") as output:
        json.dump(dict_codigos, output)


if __name__ == "__main__":
    main()
