pacman::p_load("vroom", "glue", "DBI")


# Questão 1
## Item a)
dados_path <- "./Lista_1/dados"
arquivos <- glue("{dados_path}/{list.files(dados_path)}")
primeiro <- vroom(arquivos[1], n_max = 0, show_col_types = FALSE)
todos <- vroom(
    pipe(glue("grep -i JANSSEN {dados_path}/*.csv")),
    delim = ";",
    num_threads = 4,
    show_col_types = FALSE,
    col_names = names(primeiro)
)

janssen <- dbConnect(RSQLite::SQLite(), "./Lista_2/Janssen_db.sqlite")
dbWriteTable(janssen, "janssen_all", todos)
dbDisconnect(janssen)