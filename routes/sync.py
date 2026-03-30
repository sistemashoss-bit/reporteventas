import sys
import traceback

from flask import Blueprint, jsonify, request

from utils.db import tabla_vacia, upsert_items
from utils.normalize import normalizar_para_pg, normalize_items_sync
from utils.sheets import read_base

bp = Blueprint("sync", __name__)


@bp.route("/sync-supabase", methods=["POST"])
def sync_supabase():
    try:
        data = request.get_json(force=True) or {}

        spreadsheet_id = data.get("spreadsheet_base_id")
        if not spreadsheet_id:
            return jsonify(status="error", error="Falta spreadsheet_base_id"), 400

        sheet_name = data.get("sheet_base", "BaseV")
        ventana_dias = int(data.get("ventana_dias", 60))
        tabla = data.get("tabla", "ventas_items")

        df = read_base(spreadsheet_id, sheet_name)
        total_base = len(df)

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

        df_items = normalize_items_sync(df_sync, items=9, include_extras=False)
        records = normalizar_para_pg(df_items)
        total_upserted = upsert_items(records, tabla=tabla)

        return jsonify(
            status="ok",
            modo=modo,
            filas_base_total=total_base,
            filas_ventana=len(df_sync),
            items_normalizados=len(records),
            items_upserted=total_upserted,
            rango=rango,
        )

    except ValueError as ve:
        return jsonify(status="error", error=str(ve)), 400
    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500

