import pandas as pd

from utils.normalize import normalize_items, safe_get


def reporte_general(df):
    filtered = df[
        (df["departamento"].isin(["constructora", "distribuidores"]))
        | (
            (df["departamento"] == "sucursal")
            & (
                df["tipo_de_pago"].isin(
                    ["pago total", "puerta pagada (anticipo)", "complemento"]
                )
            )
        )
    ]
    return normalize_items(filtered)


def reporte_constructora(df):
    filtered = df[df["departamento"] == "constructora"]
    return normalize_items(filtered)


def reporte_distribuidores(df):
    filtered = df[(df["departamento"] == "distribuidores") & (df["tipo_de_pago"] == "pago")]
    return normalize_items(filtered)


def reporte_sucursales(df):
    filtered = df[
        (df["departamento"] == "sucursal")
        & (df["tipo_de_pago"].isin(["pago total", "puerta pagada (anticipo)", "complemento"]))
    ]
    return normalize_items(filtered, items=6, include_extras=True)


def aggregate_by_sucursal_descripcion(df, items=6):
    rows = []

    categorias_validas = [
        "estándar_3",
        "estandar_3",
        "estándar_1",
        "estandar_1",
        "estándar_2",
        "estandar_2",
        "con_fijo_1",
        "estándar_4",
        "estandar_4",
        "con_fijo_2",
        "doble",
    ]

    descripciones_especiales = [
        "chapa inteligente 7cm (no incluye baterías)",
        "chapa inteligente 5cm (no incluye baterías)",
        "chapa",
    ]

    for _, r in df.iterrows():
        for i in range(1, items + 1):
            if i <= 3:
                cant = safe_get(r, f"cant_{i}", f"cant{i}")
            else:
                cant = safe_get(r, f"cant{i}", f"cant_{i}")

            cant = pd.to_numeric(cant, errors="coerce")
            if pd.isna(cant) or cant <= 0:
                continue

            if i <= 4 or i == 9:
                categoria = safe_get(r, f"descr{i}_1")
            else:
                categoria = safe_get(r, f"descr{i}")

            descripcion = safe_get(r, f"descr{i}_2")
            if not descripcion or pd.isna(descripcion):
                continue

            categoria_lower = str(categoria).lower().strip() if categoria else ""
            descripcion_lower = str(descripcion).lower().strip()

            if categoria_lower == "servicio":
                continue

            es_valido = (categoria_lower in categorias_validas) or (
                descripcion_lower in descripciones_especiales
            )
            if not es_valido:
                continue

            rows.append(
                {
                    "num_sucursal": safe_get(r, "num_sucursal"),
                    "sucursal": safe_get(r, "sucursal"),
                    "descripcion": descripcion,
                    "cantidad": cant,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["num_sucursal", "sucursal", "descripcion", "cantidad_total"])

    df_items = pd.DataFrame(rows)
    df_grouped = (
        df_items.groupby(["num_sucursal", "sucursal", "descripcion"], as_index=False)
        .agg({"cantidad": "sum"})
        .rename(columns={"cantidad": "cantidad_total"})
    )
    return df_grouped.sort_values("sucursal")


def reporte_maximos_general(df):
    filtered = df[
        (df["departamento"].isin(["constructora", "distribuidores"]))
        | (
            (df["departamento"] == "sucursal")
            & (
                df["tipo_de_pago"].isin(
                    ["pago total", "puerta pagada (anticipo)", "complemento"]
                )
            )
        )
    ]
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_constructora(df):
    filtered = df[df["departamento"] == "constructora"]
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_distribuidores(df):
    filtered = df[df["departamento"] == "distribuidores"]
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_sucursales(df):
    filtered = df[
        (df["departamento"] == "sucursal")
        & (df["tipo_de_pago"].isin(["pago total", "puerta pagada (anticipo)", "complemento"]))
    ]
    return aggregate_by_sucursal_descripcion(filtered)


def run_reporte(tipo, df):
    tipo = tipo.upper()
    if tipo == "GENERAL":
        return reporte_general(df)
    if tipo == "CONSTRUCTORA":
        return reporte_constructora(df)
    if tipo == "DISTRIBUIDORES":
        return reporte_distribuidores(df)
    if tipo == "SUCURSALES":
        return reporte_sucursales(df)
    raise ValueError(f"Tipo no válido: {tipo}")


def run_reporte_maximos(tipo, df):
    tipo = tipo.upper()
    if tipo == "GENERAL":
        return reporte_maximos_general(df)
    if tipo == "CONSTRUCTORA":
        return reporte_maximos_constructora(df)
    if tipo == "DISTRIBUIDORES":
        return reporte_maximos_distribuidores(df)
    if tipo == "SUCURSALES":
        return reporte_maximos_sucursales(df)
    raise ValueError(f"Tipo MAXIMOS no válido: {tipo}")

