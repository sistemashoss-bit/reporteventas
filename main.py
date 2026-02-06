from flask import Flask, request, jsonify
import gspread
import pandas as pd
from google.auth import default
import traceback
import sys

app = Flask(__name__)

creds, _ = default()
gc = gspread.authorize(creds)

# ------------------------
# LECTURA BASE
# ------------------------
def read_base(spreadsheet_id, sheet_name):
    try:
        ws = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        data = ws.get_all_records()
        
        if not data:
            raise ValueError(f"La hoja '{sheet_name}' está vacía")
        
        df = pd.DataFrame(data)
        
        # Normalización de columnas más robusta
        column_map = {}
        for col in df.columns:
            normalized = (
                str(col)
                .strip()
                .lower()
                .replace(" ", "_")
                .replace(".", "")
                .replace("-", "_")
                .replace("#", "num")
                .replace("á", "a")
                .replace("é", "e")
                .replace("í", "i")
                .replace("ó", "o")
                .replace("ú", "u")
            )
            column_map[col] = normalized
        
        df.rename(columns=column_map, inplace=True)
        
        # Logging para debug
        print(f"Columnas encontradas: {df.columns.tolist()}", file=sys.stderr)
        
        df["num_a"] = pd.to_numeric(df["num_a"], errors="coerce")
        
        if "departamento" in df.columns:
            df["departamento"] = df["departamento"].astype(str).str.strip().str.lower()
        
        if "tipo_de_pago" in df.columns:
            df["tipo_de_pago"] = df["tipo_de_pago"].astype(str).str.strip().str.lower()

        return df
    
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Hoja '{sheet_name}' no encontrada en spreadsheet {spreadsheet_id}")
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Spreadsheet {spreadsheet_id} no encontrado o sin permisos")
    except Exception as e:
        print(f"Error en read_base: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


# ------------------------
# NORMALIZACIÓN
# ------------------------
def safe_get(row, *keys):
    """Intenta obtener valor con múltiples keys posibles"""
    for key in keys:
        if key in row.index and pd.notna(row[key]):
            return row[key]
    return None


def normalize_items(df, items=9, include_extras=False):
    """
    include_extras: si True, incluye columnas adicionales para SUCURSALES
    """
    out = []

    for _, r in df.iterrows():
        for i in range(1, items + 1):
            # Manejo robusto de nombres inconsistentes
            if i <= 3:
                cant = safe_get(r, f"cant__{i}", f"cant_{i}")
            else:
                cant = safe_get(r, f"cant_{i}", f"cant__{i}")
            
            cant = pd.to_numeric(cant, errors="coerce")

            if pd.isna(cant) or cant <= 0:
                continue

            # Categoría
            if i <= 4:
                categoria = safe_get(r, f"descr{i}_1")
            elif i == 9:
                categoria = safe_get(r, f"descr9_1", f"descr{i}")
            else:  # 5,6,7,8
                categoria = safe_get(r, f"descr{i}", f"descr{i}_1")

            # Descripción
            descripcion = safe_get(r, f"descr{i}_2")

            # Precio
            if i == 7:
                precio_final = safe_get(r, f"precio_final_6", f"precio_final_{i}")
            else:
                precio_final = safe_get(r, f"precio_final_{i}")

            row = {
                "fecha_captura": safe_get(r, "fecha_captura"),
                "fecha": safe_get(r, "fecha"),
                "folio": safe_get(r, "folio"),
                "departamento": safe_get(r, "departamento"),
                "cliente": safe_get(r, "cliente"),
                "metodo_de_venta": safe_get(r, "metodo_de_venta", "metodo_de_venta"),
                "num_sucursal": safe_get(r, "num_sucursal", "num__sucursal", "_sucursal"),
                "sucursal": safe_get(r, "sucursal"),
                "vendedor": safe_get(r, "vendedor"),
                "cantidad": cant,
                "categoria": categoria,
                "descripcion": descripcion,
                "precio_final": precio_final,
                "tipo_de_pago": safe_get(r, "tipo_de_pago"),
                "salida": safe_get(r, "salida")
            }

            # Campos extras para SUCURSALES
            if include_extras:
                adicional_1 = str(safe_get(r, "adicional_1") or "").lower()
                adicional_2 = str(safe_get(r, "adicional_2") or "").lower()
                
                comentario_cupon = None
                if any(x in adicional_1 for x in ["chs", "model", "cambio", "cancel", "folio"]):
                    comentario_cupon = safe_get(r, "adicional_1")
                elif any(x in adicional_2 for x in ["chs", "model", "cambio", "cancel", "folio"]):
                    comentario_cupon = safe_get(r, "adicional_2")
                
                monto_cupon = None
                if "chs" in adicional_1:
                    monto_cupon = safe_get(r, "precio_adic_1")
                elif "chs" in adicional_2:
                    monto_cupon = safe_get(r, "precio_adic_2")
                
                comp1 = str(safe_get(r, "comp1") or "").lower()
                comp2 = str(safe_get(r, "comp2") or "").lower()
                
                comentario = None
                if any(x in comp1 for x in ["cancel", "modelo", "model", "cambio"]):
                    comentario = safe_get(r, "comp1")
                elif any(x in comp2 for x in ["cancel", "modelo", "model", "cambio"]):
                    comentario = safe_get(r, "comp2")
                
                row["comentario_cupon"] = comentario_cupon
                row["monto_cupon"] = monto_cupon
                row["comentario"] = comentario

            out.append(row)

    return pd.DataFrame(out)


# ------------------------
# REPORTES
# ------------------------
def reporte_general(df):
    filtered = df[
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
    return normalize_items(filtered)

def reporte_constructora(df):
    return normalize_items(df[df["departamento"] == "constructora"])

def reporte_distribuidores(df):
    filtered = df[
        (df["departamento"] == "distribuidores") &
        (df["tipo_de_pago"] == "pago")
    ]
    return normalize_items(filtered)

def reporte_sucursales(df):
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
    try:
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(sheet_name)
        
        # Limpiar desde fila 26 hacia abajo
        max_rows = ws.row_count
        max_cols = ws.col_count
        
        if max_rows >= start_row:
            range_to_clear = f"A{start_row}:{gspread.utils.rowcol_to_a1(max_rows, max_cols)}"
            ws.batch_clear([range_to_clear])
        
        # Escribir headers en fila 26
        headers = [[str(col).replace("_", " ").title() for col in df.columns]]
        ws.update(f"A{start_row}", headers)
        
        # Escribir datos desde fila 27
        if len(df) > 0:
            data = df.fillna("").astype(str).values.tolist()
            ws.update(f"A{start_row + 1}", data)
        
        print(f"Escritura exitosa: {len(df)} filas en '{sheet_name}'", file=sys.stderr)
    
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Hoja '{sheet_name}' no encontrada para escritura")
    except Exception as e:
        print(f"Error en write_to_sheet: {str(e)}", file=sys.stderr)
        raise


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
        
        print(f"Request recibido: {data}", file=sys.stderr)

        # Validación de parámetros
        required = ["spreadsheet_base_id", "spreadsheet_reporte_id", "fecha_ini", "fecha_fin", "tipo"]
        for field in required:
            if field not in data:
                return jsonify(status="error", error=f"Falta parámetro: {field}"), 400

        df = read_base(
            data["spreadsheet_base_id"],
            data.get("sheet_base", "BaseV")
        )

        print(f"DataFrame cargado: {len(df)} filas", file=sys.stderr)

        ini = int(data["fecha_ini"])
        fin = int(data["fecha_fin"])
        tipo = data["tipo"]

        out = ejecutar_reporte(tipo, df, ini, fin)
        
        print(f"Reporte generado: {len(out)} filas", file=sys.stderr)
        
        write_to_sheet_legacy_style(
            out,
            data["spreadsheet_reporte_id"],
            data.get("sheet_reporte", "REPORTE VENTAS"),
            start_row=26
        )

        return jsonify(status="ok", tipo=tipo, rows=len(out))

    except ValueError as ve:
        print(f"ValueError: {str(ve)}", file=sys.stderr)
        return jsonify(status="error", error=str(ve)), 400
    except Exception as e:
        print(f"Error general: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e), trace=traceback.format_exc()), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)