from flask import Flask, request, jsonify
import gspread
import pandas as pd
from google.auth import default
import traceback

app = Flask(__name__)

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
def normalize_items(df, items=9, include_extras=False):
    """
    include_extras: si True, incluye columnas adicionales para SUCURSALES
    """
    out = []

    for _, r in df.iterrows():
        for i in range(1, items + 1):
            # Manejo de nombres inconsistentes (cant__1, cant__2, cant__3 vs cant_4, cant_5...)
            if i <= 3:
                cant = r.get(f"cant__{i}")
            else:
                cant = r.get(f"cant_{i}")
            
            cant = pd.to_numeric(cant, errors="coerce")

            if pd.isna(cant) or cant <= 0:
                continue

            # Categoría (descr1_1, descr2_1, descr3_1, descr4_1, descr5, descr6...)
            if i <= 4:
                categoria = r.get(f"descr{i}_1")
            elif i == 9:
                categoria = r.get(f"descr9_1")
            else:  # 5,6,7,8
                categoria = r.get(f"descr{i}")

            # Descripción
            descripcion = r.get(f"descr{i}_2")

            # Precio (todos tienen _final_{i}, pero 7 usa precio_final_6 según legacy)
            if i == 7:
                precio_final = r.get(f"precio_final_6")
            else:
                precio_final = r.get(f"precio_final_{i}")

            row = {
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
                "categoria": categoria,
                "descripcion": descripcion,
                "precio_final": precio_final,
                "tipo_de_pago": r["tipo_de_pago"],
                "salida": r["salida"]
            }

            # Campos extras para SUCURSALES
            if include_extras:
                # Comentario Cupon
                adicional_1 = str(r.get("adicional_1", "")).lower()
                adicional_2 = str(r.get("adicional_2", "")).lower()
                
                comentario_cupon = None
                if any(x in adicional_1 for x in ["chs", "model", "cambio", "cancel", "folio"]):
                    comentario_cupon = r.get("adicional_1")
                elif any(x in adicional_2 for x in ["chs", "model", "cambio", "cancel", "folio"]):
                    comentario_cupon = r.get("adicional_2")
                
                # Monto cupon
                monto_cupon = None
                if "chs" in adicional_1:
                    monto_cupon = r.get("precio_adic_1")
                elif "chs" in adicional_2:
                    monto_cupon = r.get("precio_adic_2")
                
                # Comentario
                comp1 = str(r.get("comp1", "")).lower()
                comp2 = str(r.get("comp2", "")).lower()
                
                comentario = None
                if any(x in comp1 for x in ["cancel", "modelo", "model", "cambio"]):
                    comentario = r.get("comp1")
                elif any(x in comp2 for x in ["cancel", "modelo", "model", "cambio"]):
                    comentario = r.get("comp2")
                
                row["comentario_cupon"] = comentario_cupon
                row["monto_cupon"] = monto_cupon
                row["comentario"] = comentario

            out.append(row)

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

def reporte_sucursales(df):
    """
    Nuevo reporte para SUCURSALES con campos adicionales
    """
    filtered = df[
        (df["departamento"] == "sucursal") &
        (df["tipo_de_pago"].isin([
            "pago total",
            "puerta pagada (anticipo)",
            "complemento"
        ]))
    ]
    return normalize_items(filtered, items=6, include_extras=True)


# ------------------------
# SHEETS IO
# ------------------------
def write_to_sheet_legacy_style(df, spreadsheet_id, sheet_name, start_row=26):
    """
    Escribe en la misma hoja desde start_row (fila 26 como en legacy)
    """
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(sheet_name)
    
    # Limpiar desde fila 26 hacia abajo
    max_rows = ws.row_count
    if max_rows > start_row:
        ws.batch_clear([f"A{start_row}:{ws.col_count}{max_rows}"])
    
    # Escribir headers en fila 26
    headers = df.columns.tolist()
    ws.update(f"A{start_row}", [headers])
    
    # Escribir datos desde fila 27
    if len(df) > 0:
        data = df.fillna("").astype(str).values.tolist()
        ws.update(f"A{start_row + 1}", data)


def filtrar_por_fecha(df, ini, fin):
    return df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]


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


def ejecutar_reporte(tipo, df, ini, fin):
    df_fechas = filtrar_por_fecha(df, ini, fin)
    out = run_reporte(tipo, df_fechas)
    return out


# ------------------------
# API
# ------------------------
@app.route("/run-multi", methods=["POST"])
def run_multi():
    try:
        data = request.get_json(force=True)

        df = read_base(
            data["spreadsheet_base_id"],
            data.get("sheet_base", "BaseV")
        )

        ini = int(data["fecha_ini"])
        fin = int(data["fecha_fin"])
        tipo = data["tipo"]

        out = ejecutar_reporte(tipo, df, ini, fin)
        
        # Escribir en la misma hoja desde fila 26 (estilo legacy)
        write_to_sheet_legacy_style(
            out,
            data["spreadsheet_reporte_id"],
            data.get("sheet_reporte", "REPORTE VENTAS"),
            start_row=26
        )

        return jsonify(status="ok", tipo=tipo, rows=len(out))

    except Exception as e:
        print(traceback.format_exc())
        return jsonify(status="error", error=str(e)), 500
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)