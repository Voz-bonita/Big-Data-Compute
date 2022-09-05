from urllib.request import urlretrieve
import os


def main():
    # fmt: off
    estados = [
        "AC", "AL", "AM", "AP", "BA", "CE",
        "DF", "ES", "GO", "MA", "MG", "MS",
        "MT", "PA", "PB", "PE", "PI", "PR",
        "RJ", "RN", "RO", "RR", "RS", "SC",
        "SE", "SP", "TO"
    ]
    # fmt: on
    anos = range(1996, 2021)

    db = "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES"

    path = "./Lista_3/datasus"
    if not os.path.exists(path):
        os.mkdir(path)

    for estado in estados:
        print(estado)
        for ano in anos:
            print(ano)
            file = f"DN{estado}{ano}.dbc"
            urlretrieve(f"{db}/{file}", f"{path}/{file}")


if __name__ == "__main__":
    main()
