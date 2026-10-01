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

        print(
            f"DEBUG base: filas={len(df)} num_a_min={df['num_a'].min()} "
            f"num_a_max={df['num_a'].max()} num_a_nan={df['num_a'].isna().sum()}",
            file=sys.stderr,
        )
        print(
            f"DEBUG rango {ini}-{fin}: filas={len(df_fechas)} "
            f"num_a_max={df_fechas['num_a'].max()}",
            file=sys.stderr,
        )
        print(
            f"DEBUG filas por num_a en rango: {df_fechas['num_a'].value_counts().sort_index().to_dict()}",
            file=sys.stderr,
        )
        df_tarde = df_fechas[df_fechas["num_a"] > 46288]
        print(
            f"DEBUG despues 23/09 departamento: {df_tarde['departamento'].value_counts().to_dict()}",
            file=sys.stderr,
        )
        print(
            f"DEBUG despues 23/09 tipo_de_pago: {df_tarde['tipo_de_pago'].value_counts().to_dict()}",
            file=sys.stderr,
        )

        out = run_reporte(tipo, df_fechas)

        if "fecha" in out.columns:
            print(
                f"DEBUG salida: filas={len(out)} fechas={out['fecha'].astype(str).value_counts().to_dict()}",
                file=sys.stderr,
            )

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

