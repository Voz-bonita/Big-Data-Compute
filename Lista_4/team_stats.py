import pandas as pd


def main():
    with open("./Lista_4/team_stats.html", "r") as file:
        copa_15 = pd.read_html(file.read())[0]

    copa_15 = (
        copa_15.loc[:, ["Country", "P", "GF", "GA"]]
        .sort_values("Country")
        .set_axis(["Pais", "Jogos", "Gols_feitos", "Gols_sofridos"], axis=1)
    )

    copa_15.to_excel("./Lista_4/Estatisticas_times.xlsx", index=False)


if __name__ == "__main__":
    main()
