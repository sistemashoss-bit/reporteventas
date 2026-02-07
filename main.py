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
        
        # Normalización de columnas
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
                .replace("(", "")
                .replace(")", "")
                .replace("/", "_")
            )
            column_map[col] = normalized
        
        df.rename(columns=column_map, inplace=True)
        
        print(f"Columnas encontradas: {df.columns.tolist()[:20]}...", file=sys.stderr)
        
        df["num_a"] = pd.to_numeric(df["num_a"], errors="coerce")
        
        if "departamento" in df.columns:
            df["departamento"] = df["departamento"].astype(str).str.strip().str.lower()
        
        if "tipo_de_pago" in df.columns:
            df["tipo_de_pago"] = (
                df["tipo_de_pago"]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.replace(r'\s+', ' ', regex=True)
            )

        return df
    
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Hoja '{sheet_name}' no encontrada")
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Spreadsheet {spreadsheet_id} no encontrado")
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
    Normaliza items de ventas con soporte para nombres de columnas inconsistentes
    """
    out = []
    
    print(f"Normalizando {len(df)} filas con {items} items", file=sys.stderr)

    for _, r in df.iterrows():
        for i in range(1, items + 1):
            # Nombres reales según el debug:
            # cant_1, cant_2, cant_3, cant4, cant5, cant6, cant7, cant8, cant9
            if i <= 3:
                cant = safe_get(r, f"cant_{i}", f"cant{i}")
            else:
                cant = safe_get(r, f"cant{i}", f"cant_{i}")
            
            cant = pd.to_numeric(cant, errors="coerce")

            if pd.isna(cant) or cant <= 0:
                continue

            # Categoría: descr1_1, descr2_1, descr3_1, descr4_1, descr5, descr6...
            if i <= 4 or i == 9:
                categoria = safe_get(r, f"descr{i}_1")
            else:  # 5,6,7,8
                categoria = safe_get(r, f"descr{i}")

            # Descripción: siempre descr{i}_2
            descripcion = safe_get(r, f"descr{i}_2")

            # Precio final: siempre precio_final_{i}
            precio_final = safe_get(r, f"precio_final_{i}")

            row = {
                "fecha_captura": safe_get(r, "fecha_captura"),
                "fecha": safe_get(r, "fecha"),
                "folio": safe_get(r, "folio"),
                "departamento": safe_get(r, "departamento"),
                "cliente": safe_get(r, "cliente"),
                "metodo_de_venta": safe_get(r, "metodo_de_venta"),
                "num_sucursal": safe_get(r, "num_sucursal"),
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

    print(f"Rows normalizados: {len(out)}", file=sys.stderr)
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
    print(f"GENERAL filtrado: {len(filtered)} filas", file=sys.stderr)
    return normalize_items(filtered)


def reporte_constructora(df):
    filtered = df[df["departamento"] == "constructora"]
    print(f"CONSTRUCTORA filtrado: {len(filtered)} filas", file=sys.stderr)
    return normalize_items(filtered)


def reporte_distribuidores(df):
    filtered = df[
        (df["departamento"] == "distribuidores") &
        (df["tipo_de_pago"] == "pago")
    ]
    print(f"DISTRIBUIDORES filtrado: {len(filtered)} filas", file=sys.stderr)
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
    print(f"SUCURSALES filtrado: {len(filtered)} filas", file=sys.stderr)
    return normalize_items(filtered, items=6, include_extras=True)


# ------------------------
# SHEETS IO
# ------------------------
def write_to_sheet_legacy_style(df, spreadsheet_id, sheet_name, start_row=26):
    """
    Escribe en la misma hoja desde start_row (fila 26 como en legacy)
    Con formato correcto: fechas YYYY-MM-DD, departamento capitalizado, números sin apóstrofe
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
        
        # Headers
        header_map = {
            "fecha_captura": "Fecha Captura",
            "fecha": "Fecha",
            "folio": "Folio",
            "departamento": "Departamento",
            "cliente": "Cliente",
            "metodo_de_venta": "Metodo de Venta",
            "num_sucursal": "Num Sucursal",
            "sucursal": "Sucursal",
            "vendedor": "Vendedor",
            "cantidad": "Cantidad",
            "categoria": "Categoria",
            "descripcion": "Descripcion",
            "precio_final": "Precio Final",
            "tipo_de_pago": "Tipo de Pago",
            "salida": "Salida",
            "comentario_cupon": "Comentario Cupon",
            "monto_cupon": "Monto Cupon",
            "comentario": "Comentario"
        }
        
        headers = [[header_map.get(col, col.replace("_", " ").title()) for col in df.columns]]
        ws.update(f"A{start_row}", headers)
        
        # Formatear datos
        if len(df) > 0:
            df_formatted = df.copy()
            
            # Capitalizar departamento
            if "departamento" in df_formatted.columns:
                df_formatted["departamento"] = df_formatted["departamento"].str.capitalize()
            
            # Capitalizar tipo_de_pago (Primera letra de cada palabra)
            if "tipo_de_pago" in df_formatted.columns:
                df_formatted["tipo_de_pago"] = df_formatted["tipo_de_pago"].str.title()
            
            # Formatear fechas
            def parse_date_safe(val):
                if pd.isna(val) or str(val).strip() == "":
                    return ""
                try:
                    dt = pd.to_datetime(val, errors='coerce')
                    if pd.notna(dt):
                        return dt.strftime('%Y-%m-%d')
                except:
                    pass
                return str(val)
            
            for col in ["fecha_captura", "fecha"]:
                if col in df_formatted.columns:
                    df_formatted[col] = df_formatted[col].apply(parse_date_safe)
            
            # Convertir columnas numéricas a números (sin apóstrofe)
            numeric_cols = ["folio", "num_sucursal", "cantidad", "precio_final", "monto_cupon"]
            for col in numeric_cols:
                if col in df_formatted.columns:
                    df_formatted[col] = pd.to_numeric(df_formatted[col], errors='coerce')
            
            # Preparar datos para escribir
            data = []
            for _, row in df_formatted.iterrows():
                row_data = []
                for col in df_formatted.columns:
                    val = row[col]
                    
                    # Si es NaN o None, vacío
                    if pd.isna(val):
                        row_data.append("")
                    # Si es numérico, mantener como número
                    elif col in numeric_cols:
                        row_data.append(val if not pd.isna(val) else "")
                    # Todo lo demás como string
                    else:
                        row_data.append(str(val))
                
                data.append(row_data)
            
            # Escribir
            ws.update(f"A{start_row + 1}", data)
        
        print(f"Escritura exitosa: {len(df)} filas en '{sheet_name}'", file=sys.stderr)
    
    except Exception as e:
        print(f"Error en write_to_sheet: {str(e)}", file=sys.stderr)
        raise

def filtrar_por_fecha(df, ini, fin):
    filtered = df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]
    print(f"Filtro de fecha {ini}-{fin}: {len(filtered)} filas (de {len(df)} totales)", file=sys.stderr)
    print(f"Rango real en datos: {df['num_a'].min():.0f} - {df['num_a'].max():.0f}", file=sys.stderr)
    return filtered


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
        
        print(f"Request: {data}", file=sys.stderr)

        # Validación
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
        print(f"Error: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500


@app.route("/debug", methods=["POST"])
def debug():
    try:
        data = request.get_json(force=True)
        
        df = read_base(
            data["spreadsheet_base_id"],
            data.get("sheet_base", "BaseV")
        )
        
        ini = int(data["fecha_ini"])
        fin = int(data["fecha_fin"])
        
        df_fechas = df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]
        
        info = {
            "total_rows": len(df),
            "rows_after_date_filter": len(df_fechas),
            "date_range": f"{ini} - {fin}",
            "departamentos_unicos": df["departamento"].value_counts().to_dict() if "departamento" in df.columns else {},
            "tipo_pago_unicos": df["tipo_de_pago"].value_counts().to_dict() if "tipo_de_pago" in df.columns else {},
            "num_a_min": int(df["num_a"].min()) if len(df) > 0 else None,
            "num_a_max": int(df["num_a"].max()) if len(df) > 0 else None,
            "sample_departamentos": df["departamento"].head(10).tolist() if "departamento" in df.columns else [],
            "sample_tipo_pago": df["tipo_de_pago"].head(10).tolist() if "tipo_de_pago" in df.columns else [],
            "columns": df.columns.tolist()
        }
        
        if data.get("tipo") == "SUCURSALES":
            sucursal_df = df[df["departamento"] == "sucursal"]
            info["sucursal_rows"] = len(sucursal_df)
            info["sucursal_tipo_pago"] = sucursal_df["tipo_de_pago"].value_counts().to_dict() if len(sucursal_df) > 0 else {}
            
            for i in range(1, 7):
                if i <= 3:
                    col_cant = f"cant_{i}"
                else:
                    col_cant = f"cant{i}"
                    
                if col_cant in df.columns:
                    non_zero = df[col_cant].apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()
                    non_zero = non_zero[non_zero > 0]
                    info[f"{col_cant}_non_zero_count"] = len(non_zero)
        
        return jsonify(status="ok", debug=info)
        
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)