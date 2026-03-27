from datetime import datetime

import pandas as pd


def safe_get(row, *keys):
    for key in keys:
        if key in row.index and pd.notna(row[key]):
            return row[key]
    return None


def normalize_items(df, items=9, include_extras=False):
    out = []

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
                "salida": safe_get(r, "salida"),
            }

            if include_extras:
                adicional_1 = str(safe_get(r, "adicional_1") or "").lower()
                adicional_2 = str(safe_get(r, "adicional_2") or "").lower()

                comentario_cupon = None
                if any(x in adicional_1 for x in ["chs", "model", "cambio", "cancel", "folio"]):
                    comentario_cupon = safe_get(r, "adicional_1")
                elif any(
                    x in adicional_2 for x in ["chs", "model", "cambio", "cancel", "folio"]
                ):
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


def normalizar_para_pg(df_items: pd.DataFrame) -> list:
    records = []
    for item_index, row in enumerate(df_items.itertuples(index=False), start=1):

        def s(col):
            v = getattr(row, col, None)
            return (
                None
                if v is None or (isinstance(v, float) and pd.isna(v))
                else str(v).strip()
            )

        def n(col):
            v = getattr(row, col, None)
            if v is None:
                return None
            x = pd.to_numeric(v, errors="coerce")
            return None if pd.isna(x) else float(x)

        def d(col):
            v = getattr(row, col, None)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            dt = pd.to_datetime(v, errors="coerce")
            return None if pd.isna(dt) else dt.strftime("%Y-%m-%d")

        records.append(
            {
                "folio": s("folio"),
                "item_index": item_index,
                "fecha_captura": d("fecha_captura"),
                "fecha": d("fecha"),
                "departamento": s("departamento"),
                "cliente": s("cliente"),
                "metodo_de_venta": s("metodo_de_venta"),
                "num_sucursal": n("num_sucursal"),
                "sucursal": s("sucursal"),
                "vendedor": s("vendedor"),
                "cantidad": n("cantidad"),
                "categoria": s("categoria"),
                "descripcion": s("descripcion"),
                "precio_final": n("precio_final"),
                "tipo_de_pago": s("tipo_de_pago"),
                "salida": s("salida"),
                "synced_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return records


def filtrar_por_fecha(df, ini, fin):
    return df[(df["num_a"] >= ini) & (df["num_a"] <= fin)]

