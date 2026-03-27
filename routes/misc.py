import sys
import traceback

from flask import Blueprint, jsonify, request

from utils.sheets import read_base

bp = Blueprint("misc", __name__)


@bp.route("/debug", methods=["POST"])
def debug():
    try:
        data = request.get_json(force=True)

        df = read_base(data["spreadsheet_base_id"], data.get("sheet_base", "BaseV"))

        ini = int(data["fecha_ini"])
        fin = int(data["fecha_fin"])

        df_fechas = df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]

        info = {
            "total_rows": len(df),
            "rows_after_date_filter": len(df_fechas),
            "date_range": f"{ini} - {fin}",
            "departamentos_unicos": df["departamento"].value_counts().to_dict()
            if "departamento" in df.columns
            else {},
            "tipo_pago_unicos": df["tipo_de_pago"].value_counts().to_dict()
            if "tipo_de_pago" in df.columns
            else {},
            "num_a_min": int(df["num_a"].min()) if len(df) > 0 else None,
            "num_a_max": int(df["num_a"].max()) if len(df) > 0 else None,
            "columns": df.columns.tolist(),
        }

        return jsonify(status="ok", debug=info)

    except Exception as e:
        print(traceback.format_exc(), file=sys.stderr)
        return jsonify(status="error", error=str(e)), 500


@bp.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")

