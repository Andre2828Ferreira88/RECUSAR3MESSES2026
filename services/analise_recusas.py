import pandas as pd
import os

def ler_arquivo(caminho):
    ext = os.path.splitext(caminho)[1].lower()
    if ext == ".csv":
        try:
            return pd.read_csv(caminho, encoding="utf-8", engine="python", sep=None)
        except Exception:
            return pd.read_csv(caminho, encoding="latin1", engine="python", sep=None)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(caminho)
    else:
        raise ValueError("Formato não suportado. Use CSV ou XLSX.")

def apenas_recusou(df):
    g = (
        df.groupby(["COD_PRESTADOR", "Grupo de Serviços"])["RECUSA"]
        .apply(lambda x: set(x.astype(str).str.strip()) == {"1"})
        .reset_index(name="SO_RECUSOU")
    )
    g = g[g["SO_RECUSOU"]]
    g["CHAVE"] = (
        g["COD_PRESTADOR"].astype(str).str.strip()
        + "_" +
        g["Grupo de Serviços"].astype(str).str.strip()
    )
    return g[["COD_PRESTADOR", "Grupo de Serviços", "CHAVE"]]

def processar(m1, m2, m3, codigos):
    df1 = ler_arquivo(m1)
    df2 = ler_arquivo(m2)
    df3 = ler_arquivo(m3)
    df_cod = ler_arquivo(codigos)

    obrigatorias = ["Grupo de Serviços", "COD_PRESTADOR", "RECUSA"]
    for df in [df1, df2, df3]:
        for c in obrigatorias:
            if c not in df.columns:
                raise ValueError(f"Coluna obrigatória ausente: {c}")

    for df in [df1, df2, df3]:
        for col in obrigatorias:
            df[col] = df[col].astype(str).str.strip()

    df_cod["COD_PRESTADOR"] = df_cod["COD_PRESTADOR"].astype(str).str.strip()
    df_cod["RAZAO_SOCIAL"] = df_cod["RAZAO_SOCIAL"].astype(str).str.strip()
    df_cod = df_cod.drop_duplicates("COD_PRESTADOR")

    r1 = apenas_recusou(df1)
    r2 = apenas_recusou(df2)
    r3 = apenas_recusou(df3)

    inter = set(r1["CHAVE"]) & set(r2["CHAVE"]) & set(r3["CHAVE"])

    if inter:
        res = pd.DataFrame({"CHAVE": list(inter)})
        res[["COD_PRESTADOR", "Grupo de Serviços"]] = (
            res["CHAVE"].str.split(pat="_", n=1, expand=True)
        )

        res["Quantidade de recusas"] = 3
    else:
        res = pd.DataFrame(columns=["COD_PRESTADOR", "Grupo de Serviços", "Quantidade de recusas"])

    res = res.merge(
        df_cod[["COD_PRESTADOR", "RAZAO_SOCIAL"]],
        on="COD_PRESTADOR",
        how="left"
    )

    res = res[[
        "COD_PRESTADOR",
        "RAZAO_SOCIAL",
        "Grupo de Serviços",
        "Quantidade de recusas"
    ]]

    return res
