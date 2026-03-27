import sys
import traceback

import gspread
import pandas as pd
from google.auth import default

_gc = None


def get_gspread_client():
    global _gc
    if _gc is None:
        creds, _ = default()
        _gc = gspread.authorize(creds)
    return _gc


def read_base(spreadsheet_id, sheet_name):
    try:
        gc = get_gspread_client()
        ws = gc.open_by_key(spreadsheet_id).worksheet(sheet_name)
        data = ws.get_all_records()

        if not data:
            raise ValueError(f"La hoja '{sheet_name}' está vacía")

        df = pd.DataFrame(data)

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
                .str.replace(r"\s+", " ", regex=True)
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


def write_to_sheet_legacy_style(df, spreadsheet_id, sheet_name, start_row=26):
    try:
        gc = get_gspread_client()
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
            "comentario": "Comentario",
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
                    dt = pd.to_datetime(val, errors="coerce")
                    if pd.notna(dt):
                        return dt.strftime("%Y-%m-%d")
                except Exception:
                    pass
                return str(val)

            for col in ["fecha_captura", "fecha"]:
                if col in df_formatted.columns:
                    df_formatted[col] = df_formatted[col].apply(parse_date_safe)

            numeric_cols = ["folio", "num_sucursal", "cantidad", "precio_final", "monto_cupon"]
            for col in numeric_cols:
                if col in df_formatted.columns:
                    df_formatted[col] = pd.to_numeric(df_formatted[col], errors="coerce")

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


def write_to_sheet_maximos(df, spreadsheet_id, sheet_name, start_row=12):
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(sheet_name)

        max_rows = ws.row_count
        max_cols = ws.col_count

        if max_rows >= start_row:
            range_to_clear = f"A{start_row}:{gspread.utils.rowcol_to_a1(max_rows, max_cols)}"
            ws.batch_clear([range_to_clear])

        headers = [["Numero de Sucursal", "Sucursal", "Descripcion", "Cantidad Total"]]
        ws.update(f"A{start_row}", headers)

        if len(df) > 0:
            df_formatted = df.copy()

            numeric_cols = ["num_sucursal", "cantidad_total"]
            for col in numeric_cols:
                if col in df_formatted.columns:
                    df_formatted[col] = pd.to_numeric(df_formatted[col], errors="coerce")

            data = []
            for _, row in df_formatted.iterrows():
                row_data = [
                    row["num_sucursal"] if not pd.isna(row["num_sucursal"]) else "",
                    str(row["sucursal"]) if not pd.isna(row["sucursal"]) else "",
                    str(row["descripcion"]) if not pd.isna(row["descripcion"]) else "",
                    row["cantidad_total"] if not pd.isna(row["cantidad_total"]) else "",
                ]
                data.append(row_data)

            ws.update(f"A{start_row + 1}", data)

        print(f"Escritura MAXIMOS exitosa: {len(df)} filas en '{sheet_name}'", file=sys.stderr)

    except Exception as e:
        print(f"Error en write_to_sheet_maximos: {str(e)}", file=sys.stderr)
        raise

