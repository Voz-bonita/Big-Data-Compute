import requests
import json


def main():
    XHR_header = "lista.php?output=jsonbt&&order=asc&_=1659303484372"
    IBGE = f"https://sage.saude.gov.br/paineis/regiaoSaude/{XHR_header}"
    response = requests.get(IBGE)
    mapa_codigos = response.json()

    dict_codigos = {
        registro["ibge"]: registro["co_colegiado"] for registro in mapa_codigos
    }

    with open("./Lista_1/mapa_codigos.json", "w") as output:
        json.dump(dict_codigos, output)


if __name__ == "__main__":
    main()
