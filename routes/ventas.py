import sys
import traceback

from flask import Blueprint, jsonify, request

from utils.normalize import filtrar_por_fecha
from utils.reports import run_reporte
from utils.sheets import read_base, write_to_sheet_legacy_style

bp = Blueprint("ventas", __name__)


@bp.route("/run-multi", methods=["POST"])
def run_multi():
    try:
        data = request.get_json(force=True)
        print(f"Request VENTAS: {data}", file=sys.stderr)

        required = [
            "spreadsheet_base_id",
            "spreadsheet_reporte_id",
            "fecha_ini",
            "fecha_fin",
            "tipo",
        ]
        for field in required:
            if field not in data:
                return jsonify(status="error", error=f"Falta parámetro: {field}"), 400

        df = read_base(data["spreadsheet_base_id"], data.get("sheet_base", "BaseV"))

        ini = int(data["fecha_ini"])
        fin = int(data["fecha_fin"])
        tipo = data["tipo"]

        df_fechas = filtrar_por_fecha(df, ini, fin)
        out = run_reporte(tipo, df_fechas)

        write_to_sheet_legacy_style(
            out,
            data["spreadsheet_reporte_id"],
            data.get("sheet_reporte", "REPORTE VENTAS"),
            start_row=26,
        )

        return jsonify(status="ok", tipo=tipo, rows=len(out))

    except ValueError as ve:
        print(f"ValueError: {str(ve)}", file=sys.stderr)
        return jsonify(status="error", error=str(ve)), 400
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500

