import os
import sys
import pandas as pd
import gspread
import traceback
import psycopg2
import psycopg2.pool
import psycopg2.extras
from datetime import datetime
from flask import Flask, request, jsonify
from google.auth import default


app = Flask(__name__)

SYNC_DB_URL = os.environ.get("DATABASE_URL")
_sync_pool = None

creds, _ = default()
gc = gspread.authorize(creds)

# ------------------------
# Supabase y Nuevo Cron
# ------------------------

def get_sync_pool():
    global _sync_pool
    if _sync_pool is None:
        _sync_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=5, dsn=SYNC_DB_URL, connect_timeout=5
        )
    return _sync_pool

def get_sync_conn():
    return get_sync_pool().getconn()

def release_sync_conn(conn):
    get_sync_pool().putconn(conn)


def tabla_vacia(tabla="ventas_items"):
    conn = get_sync_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT EXISTS (SELECT 1 FROM {tabla} LIMIT 1)")
            tiene_datos = cur.fetchone()[0]
            return not tiene_datos
    finally:
        release_sync_conn(conn)


def upsert_items(records: list, tabla="ventas_items", batch_size=500):
    if not records:
        return 0

    columnas = list(records[0].keys())
    cols_str = ", ".join(columnas)

    update_cols = [c for c in columnas if c not in ("folio", "item_index")]
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    query = f"""
        INSERT INTO {tabla} ({cols_str})
        VALUES %s
        ON CONFLICT (folio, item_index) DO UPDATE SET {update_str}
    """

    conn = get_sync_conn()
    total = 0
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            for start in range(0, len(records), batch_size):
                batch = records[start : start + batch_size]
                valores = [tuple(r[c] for c in columnas) for r in batch]
                psycopg2.extras.execute_values(cur, query, valores)
                total += len(batch)
                print(f"Upsert {start}–{start+len(batch)}: OK", file=sys.stderr)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_sync_conn(conn)

    return total


def normalizar_para_pg(df_items: pd.DataFrame) -> list:
    records = []
    for item_index, row in enumerate(df_items.itertuples(index=False), start=1):

        def s(col):
            v = getattr(row, col, None)
            return None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()

        def n(col):
            v = getattr(row, col, None)
            if v is None: return None
            x = pd.to_numeric(v, errors="coerce")
            return None if pd.isna(x) else float(x)

        def d(col):
            v = getattr(row, col, None)
            if v is None or (isinstance(v, float) and pd.isna(v)): return None
            try:
                dt = pd.to_datetime(v, errors="coerce")
                return None if pd.isna(dt) else dt.strftime("%Y-%m-%d")
            except:
                return None

        records.append({
            "folio":            s("folio"),      # TEXT en tu tabla
            "item_index":       item_index,      # coincide con UNIQUE (folio, item_index)
            "fecha_captura":    d("fecha_captura"),
            "fecha":            d("fecha"),
            "departamento":     s("departamento"),
            "cliente":          s("cliente"),
            "metodo_de_venta":  s("metodo_de_venta"),
            "num_sucursal":     n("num_sucursal"),
            "sucursal":         s("sucursal"),
            "vendedor":         s("vendedor"),
            "cantidad":         n("cantidad"),
            "categoria":        s("categoria"),
            "descripcion":      s("descripcion"),
            "precio_final":     n("precio_final"),
            "tipo_de_pago":     s("tipo_de_pago"),
            "salida":           s("salida"),
            "synced_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        })
    return records


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
# REPORTES VENTAS
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
# REPORTES MAXIMOS (AGREGADOS)
# ------------------------
def reporte_maximos_general(df):
    """Reporte MAXIMOS para GENERAL"""
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
    print(f"MAXIMOS GENERAL filtrado: {len(filtered)} filas", file=sys.stderr)
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_constructora(df):
    """Reporte MAXIMOS para CONSTRUCTORA"""
    filtered = df[df["departamento"] == "constructora"]
    print(f"MAXIMOS CONSTRUCTORA filtrado: {len(filtered)} filas", file=sys.stderr)
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_distribuidores(df):
    """Reporte MAXIMOS para DISTRIBUIDORES"""
    filtered = df[df["departamento"] == "distribuidores"]
    print(f"MAXIMOS DISTRIBUIDORES filtrado: {len(filtered)} filas", file=sys.stderr)
    return aggregate_by_sucursal_descripcion(filtered)


def reporte_maximos_sucursales(df):
    """Reporte MAXIMOS para SUCURSALES"""
    filtered = df[
        (df["departamento"] == "sucursal") &
        (df["tipo_de_pago"].isin([
            "pago total",
            "puerta pagada (anticipo)",
            "complemento"
        ]))
    ]
    print(f"MAXIMOS SUCURSALES filtrado: {len(filtered)} filas", file=sys.stderr)
    return aggregate_by_sucursal_descripcion(filtered)


def aggregate_by_sucursal_descripcion(df, items=6):
    """
    Agrupa por Sucursal + Descripción y suma cantidades
    Filtra solo categorías específicas de productos
    """
    rows = []
    
    # Categorías válidas (normalizadas a lowercase con guion bajo)
    categorias_validas = [
        "estándar_3", "estandar_3",
        "estándar_1", "estandar_1",
        "estándar_2", "estandar_2",
        "con_fijo_1",
        "estándar_4", "estandar_4",
        "con_fijo_2",
        "doble"
    ]
    
    # Descripciones especiales
    descripciones_especiales = [
        "chapa inteligente 7cm (no incluye baterías)",
        "chapa inteligente 5cm (no incluye baterías)",
        "chapa"
    ]
    
    for _, r in df.iterrows():
        for i in range(1, items + 1):
            # Cantidad
            if i <= 3:
                cant = safe_get(r, f"cant_{i}", f"cant{i}")
            else:
                cant = safe_get(r, f"cant{i}", f"cant_{i}")
            
            cant = pd.to_numeric(cant, errors="coerce")
            if pd.isna(cant) or cant <= 0:
                continue
            
            # Categoría
            if i <= 4 or i == 9:
                categoria = safe_get(r, f"descr{i}_1")
            else:
                categoria = safe_get(r, f"descr{i}")
            
            # Descripción
            descripcion = safe_get(r, f"descr{i}_2")
            
            if not descripcion or pd.isna(descripcion):
                continue
            
            # Normalizar para comparación
            categoria_lower = str(categoria).lower().strip() if categoria else ""
            descripcion_lower = str(descripcion).lower().strip()
            
            # Excluir "Servicio"
            if categoria_lower == "servicio":
                continue
            
            # Validar si cumple alguna condición
            es_valido = (
                categoria_lower in categorias_validas or
                descripcion_lower in descripciones_especiales
            )
            
            if not es_valido:
                continue
            
            rows.append({
                "num_sucursal": safe_get(r, "num_sucursal"),
                "sucursal": safe_get(r, "sucursal"),
                "descripcion": descripcion,
                "cantidad": cant
            })
    
    if not rows:
        print("No hay filas válidas después de filtrar MAXIMOS", file=sys.stderr)
        return pd.DataFrame(columns=["num_sucursal", "sucursal", "descripcion", "cantidad_total"])
    
    df_items = pd.DataFrame(rows)
    
    # Agrupar y sumar
    df_grouped = (
        df_items
        .groupby(["num_sucursal", "sucursal", "descripcion"], as_index=False)
        .agg({"cantidad": "sum"})
        .rename(columns={"cantidad": "cantidad_total"})
    )
    
    # Ordenar por sucursal
    df_grouped = df_grouped.sort_values("sucursal")
    
    print(f"Rows agregados MAXIMOS: {len(df_grouped)}", file=sys.stderr)
    return df_grouped


# ------------------------
# SHEETS IO - VENTAS
# ------------------------
def write_to_sheet_legacy_style(df, spreadsheet_id, sheet_name, start_row=26):
    """
    Escribe reportes de VENTAS desde fila 26
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(sheet_name)
        
        max_rows = ws.row_count
        max_cols = ws.col_count
        
        if max_rows >= start_row:
            range_to_clear = f"A{start_row}:{gspread.utils.rowcol_to_a1(max_rows, max_cols)}"
            ws.batch_clear([range_to_clear])
        
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
        
        if len(df) > 0:
            df_formatted = df.copy()
            
            if "departamento" in df_formatted.columns:
                df_formatted["departamento"] = df_formatted["departamento"].str.capitalize()
            
            if "tipo_de_pago" in df_formatted.columns:
                df_formatted["tipo_de_pago"] = df_formatted["tipo_de_pago"].str.title()
            
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
            
            numeric_cols = ["folio", "num_sucursal", "cantidad", "precio_final", "monto_cupon"]
            for col in numeric_cols:
                if col in df_formatted.columns:
                    df_formatted[col] = pd.to_numeric(df_formatted[col], errors='coerce')
            
            data = []
            for _, row in df_formatted.iterrows():
                row_data = []
                for col in df_formatted.columns:
                    val = row[col]
                    
                    if pd.isna(val):
                        row_data.append("")
                    elif col in numeric_cols:
                        row_data.append(val if not pd.isna(val) else "")
                    else:
                        row_data.append(str(val))
                
                data.append(row_data)
            
            ws.update(f"A{start_row + 1}", data)
        
        print(f"Escritura VENTAS exitosa: {len(df)} filas en '{sheet_name}'", file=sys.stderr)
    
    except Exception as e:
        print(f"Error en write_to_sheet: {str(e)}", file=sys.stderr)
        raise


# ------------------------
# SHEETS IO - MAXIMOS
# ------------------------
def write_to_sheet_maximos(df, spreadsheet_id, sheet_name, start_row=12):
    """
    Escribe reportes MAXIMOS desde fila 12
    Formato: Numero de Sucursal, Sucursal, Descripcion, Cantidad Total
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(sheet_name)
        
        max_rows = ws.row_count
        max_cols = ws.col_count
        
        if max_rows >= start_row:
            range_to_clear = f"A{start_row}:{gspread.utils.rowcol_to_a1(max_rows, max_cols)}"
            ws.batch_clear([range_to_clear])
        
        # Headers fijos para MAXIMOS
        headers = [["Numero de Sucursal", "Sucursal", "Descripcion", "Cantidad Total"]]
        ws.update(f"A{start_row}", headers)
        
        if len(df) > 0:
            df_formatted = df.copy()
            
            # Convertir num_sucursal y cantidad_total a números
            numeric_cols = ["num_sucursal", "cantidad_total"]
            for col in numeric_cols:
                if col in df_formatted.columns:
                    df_formatted[col] = pd.to_numeric(df_formatted[col], errors='coerce')
            
            # Preparar datos
            data = []
            for _, row in df_formatted.iterrows():
                row_data = [
                    row["num_sucursal"] if not pd.isna(row["num_sucursal"]) else "",
                    str(row["sucursal"]) if not pd.isna(row["sucursal"]) else "",
                    str(row["descripcion"]) if not pd.isna(row["descripcion"]) else "",
                    row["cantidad_total"] if not pd.isna(row["cantidad_total"]) else ""
                ]
                data.append(row_data)
            
            ws.update(f"A{start_row + 1}", data)
        
        print(f"Escritura MAXIMOS exitosa: {len(df)} filas en '{sheet_name}'", file=sys.stderr)
    
    except Exception as e:
        print(f"Error en write_to_sheet_maximos: {str(e)}", file=sys.stderr)
        raise


# ------------------------
# HELPERS
# ------------------------
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


def ejecutar_reporte(tipo, df, ini, fin):
    df_fechas = filtrar_por_fecha(df, ini, fin)
    out = run_reporte(tipo, df_fechas)
    return out


def ejecutar_reporte_maximos(tipo, df, ini, fin):
    df_fechas = filtrar_por_fecha(df, ini, fin)
    out = run_reporte_maximos(tipo, df_fechas)
    return out


# ------------------------
# API ENDPOINTS
# ------------------------
@app.route("/run-multi", methods=["POST"])
def run_multi():
    """Endpoint para reportes de VENTAS"""
    try:
        data = request.get_json(force=True)
        
        print(f"Request VENTAS: {data}", file=sys.stderr)

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


@app.route("/run-maximos", methods=["POST"])
def run_maximos():
    """Endpoint dedicado para reportes MAXIMOS"""
    try:
        data = request.get_json(force=True)
        
        print(f"Request MAXIMOS: {data}", file=sys.stderr)

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

        out = ejecutar_reporte_maximos(tipo, df, ini, fin)
        
        write_to_sheet_maximos(
            out,
            data["spreadsheet_reporte_id"],
            data.get("sheet_reporte", "MAXIMOS"),
            start_row=12
        )

        return jsonify(status="ok", tipo=tipo, rows=len(out))

    except ValueError as ve:
        print(f"ValueError MAXIMOS: {str(ve)}", file=sys.stderr)
        return jsonify(status="error", error=str(ve)), 400
    except Exception as e:
        print(f"Error MAXIMOS: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500
    

# ------------------------
# ENDPOINT CRON
# ------------------------
@app.route("/sync-supabase", methods=["POST"])
def sync_supabase():
    """
    Llamado por Google Cloud Scheduler.

    Body JSON:
    {
      "spreadsheet_base_id": "...",
      "sheet_base": "BaseV",    // opcional, default BaseV
      "ventana_dias": 60        // opcional, default 60 — ignorado en primera sync
    }
    """
    try:
        data = request.get_json(force=True) or {}

        spreadsheet_id = data.get("spreadsheet_base_id")
        if not spreadsheet_id:
            return jsonify(status="error", error="Falta spreadsheet_base_id"), 400

        sheet_name   = data.get("sheet_base", "BaseV")
        ventana_dias = int(data.get("ventana_dias", 60))
        tabla        = data.get("tabla", "ventas_items")

        # 1. Leer base completa del Drive
        df = read_base(spreadsheet_id, sheet_name)
        total_base = len(df)

        # 2. Decidir modo: FULL (primera vez) o DELTA (ventana deslizante)
        es_primera_sync = tabla_vacia(tabla)
        modo = "full" if es_primera_sync else "delta"

        if es_primera_sync:
            df_sync = df
            rango = f"0 – {df['num_a'].max():.0f}"
            print(f"MODO FULL: {len(df_sync)} filas", file=sys.stderr)
        else:
            num_a_max = df["num_a"].max()
            num_a_min = num_a_max - ventana_dias
            df_sync = df[(df["num_a"] >= num_a_min) & (df["num_a"] <= num_a_max)]
            rango = f"{num_a_min:.0f} – {num_a_max:.0f}"
            print(f"MODO DELTA: {len(df_sync)} filas (ventana {rango})", file=sys.stderr)

        # 3. Normalizar items
        df_items = normalize_items(df_sync, items=9, include_extras=True)

        # 4. Convertir a records para PostgreSQL
        records = normalizar_para_pg(df_items)

        # 5. Upsert
        total_upserted = upsert_items(records, tabla=tabla)

        return jsonify(
            status="ok",
            modo=modo,
            filas_base_total=total_base,
            filas_ventana=len(df_sync),
            items_normalizados=len(records),
            items_upserted=total_upserted,
            rango=rango
        )

    except ValueError as ve:
        return jsonify(status="error", error=str(ve)), 400
    except Exception as e:
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
            "columns": df.columns.tolist()
        }
        
        return jsonify(status="ok", debug=info)
        
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)