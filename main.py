from flask import Flask, request, jsonify
import gspread
import pandas as pd
from google.auth import default

app = Flask(__name__)

# Auth vía Service Account (Cloud Run)
creds, _ = default()
gc = gspread.authorize(creds)

# ------------------------
# LECTURA BASE
# ------------------------
def read_base(spreadsheet_id, sheet_name):
    ws = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_records())

    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" ", "_")
          .str.replace(".", "", regex=False)
          .str.replace("-", "_")
    )

    df["num_a"] = pd.to_numeric(df["num_a"], errors="coerce")
    df["departamento"] = df["departamento"].str.strip().str.lower()
    df["tipo_de_pago"] = df["tipo_de_pago"].str.strip().str.lower()

    return df


# ------------------------
# NORMALIZACIÓN
# ------------------------
def normalize_items(df, items=9):
    out = []

    for _, r in df.iterrows():
        for i in range(1, items + 1):
            cant = r.get(f"cant_{i}") or r.get(f"cant{i}")
            cant = pd.to_numeric(cant, errors="coerce")

            if pd.isna(cant) or cant <= 0:
                continue

            out.append({
                "fecha_captura": r["fecha_captura"],
                "fecha": r["fecha"],
                "folio": r["folio"],
                "departamento": r["departamento"],
                "cliente": r["cliente"],
                "metodo_de_venta": r["método_de_venta"],
                "num_sucursal": r["#_sucursal"],
                "sucursal": r["sucursal"],
                "vendedor": r["vendedor"],
                "cantidad": cant,
                "categoria": r.get(f"descr{i}_1") or r.get(f"descr{i}"),
                "descripcion": r.get(f"descr{i}_2"),
                "precio_final": r.get(f"precio_final_{i}"),
                "tipo_de_pago": r["tipo_de_pago"],
                "salida": r["salida"]
            })

    return pd.DataFrame(out)


# ------------------------
# REPORTES
# ------------------------
def reporte_general(df):
    return normalize_items(
        df[
            (df["departamento"].isin(["constructora", "distribuidores"])) |
            (
                (df["departamento"] == "sucursal") &
                (df["tipo_de_pago"].isin([
                    "pago total",
                    "puerta pagada (anticipo)",
                    "complemento"
                ]))
            )
        ]
    )

def reporte_constructora(df):
    return normalize_items(df[df["departamento"] == "constructora"])

def reporte_distribuidores(df):
    return normalize_items(
        df[
            (df["departamento"] == "distribuidores") &
            (df["tipo_de_pago"] == "pago")
        ]
    )


# ------------------------
# SHEETS IO
# ------------------------
def write_to_sheet(df, spreadsheet_id, sheet_name):
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)

    ws.update(
        [df.columns.tolist()] +
        df.fillna("").astype(str).values.tolist()
    )


def read_fechas(spreadsheet_id, sheet_name):
    ws = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
    return int(ws.acell("B3").value), int(ws.acell("B5").value)


def filtrar_por_fecha(df, ini, fin):
    return df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]


def run_reporte(tipo, df):
    if tipo == "GENERAL":
        return reporte_general(df)
    if tipo == "CONSTRUCTORA":
        return reporte_constructora(df)
    if tipo == "DISTRIBUIDORES":
        return reporte_distribuidores(df)
    raise ValueError("Tipo no válido")


def ejecutar_reporte(tipo, df, spreadsheet_id, sheet_fechas, sheet_salida):
    ini, fin = read_fechas(spreadsheet_id, sheet_fechas)
    out = run_reporte(tipo, filtrar_por_fecha(df, ini, fin))
    write_to_sheet(out, spreadsheet_id, sheet_salida)
    return len(out)


# ------------------------
# API
# ------------------------
@app.route("/run-multi", methods=["POST"])
def run_multi():
    try:
        data = request.get_json(force=True)

        df = read_base(data["spreadsheet_id"], data.get("sheet_base", "BaseV"))

        resultados = {}
        for tipo in data["reportes"]:
            resultados[tipo] = ejecutar_reporte(
                tipo,
                df,
                data["spreadsheet_id"],
                data.get("sheet_reporte", "REPORTE VENTAS"),
                f'{data.get("sheet_reporte", "REPORTE VENTAS")}_{tipo}'
            )

        return jsonify(status="ok", resultados=resultados)

    except Exception as e:
        return jsonify(status="error", error=str(e)), 500


if __name__ == "__main__":
    pass