import os
import re
import json
from decimal import Decimal
from functools import wraps
from typing import Optional

import httpx
import pandas as pd
import numpy as np
from flask import Blueprint, jsonify, request, Response

from utils.db import get_sync_conn, release_sync_conn

bp = Blueprint("mcp", __name__)

SUPABASE_URL = "https://rhzifpleopaooomcgdqv.supabase.co"
WHITELIST_EMAILS = os.environ.get("WHITELIST_EMAILS", "sistemashoss@gmail.com,felipework771@gmail.com")
whitelist = [email.strip().lower() for email in WHITELIST_EMAILS.split(",") if email.strip()]


def verify_token(token: str) -> dict | None:
    if not token:
        return None
    
    try:
        response = httpx.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5
        )
        if response.status_code != 200:
            return None
        
        user_data = response.json()
        email = user_data.get("email", "").lower()
        
        if email not in whitelist:
            return None
        
        return user_data
    except Exception:
        return None


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401
        
        token = auth_header[7:]
        
        if not token:
            return jsonify({"error": "Missing token"}), 401
        
        user = verify_token(token)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        request.user = user
        return f(*args, **kwargs)
    
    return decorated

VENTAS_SCHEMA = {
    "folio": "string - ID único de transacción",
    "item_index": "integer - Índice del item dentro del folio",
    "fecha_captura": "date - Fecha cuando se capturó en el sistema",
    "fecha": "date - Fecha de la venta",
    "departamento": "string - Departamento de la tienda",
    "cliente": "string - Nombre del cliente",
    "metodo_de_venta": "string - Método de venta (presencial, en línea, etc.)",
    "num_sucursal": "integer - Número de sucursal",
    "sucursal": "string - Nombre de la sucursal",
    "vendedor": "string - Nombre del vendedor",
    "cantidad": "integer - Cantidad de productos",
    "categoria": "string - Categoría del producto",
    "descripcion": "string - Descripción del producto",
    "precio_final": "float - Precio final de la venta",
    "tipo_de_pago": "string - Tipo de pago (efectivo, tarjeta, etc.)",
    "salida": "string - Tipo de salida",
    "comentario_cupon": "string - Comentario de cupón (si aplica)",
    "monto_cupon": "float - Monto del cupón (si aplica)",
    "comentario": "string - Comentario adicional",
    "synced_at": "datetime - Fecha de última sincronización",
}


FORBIDDEN_KEYWORDS = [
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bDELETE\b',
    r'\bDROP\b',
    r'\bCREATE\b',
    r'\bALTER\b',
    r'\bTRUNCATE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bEXECUTE\b',
    r'\bCOPY\b',
    r'\bPG_READ_FILE\b',
    r'\bPG_WRITE_FILE\b',
    r'\bLO_IMPORT\b',
    r'\bLO_EXPORT\b',
]

MAX_ROWS = 10000


def validate_query(query: str) -> tuple[bool, str]:
    query_upper = query.upper()
    
    for pattern in FORBIDDEN_KEYWORDS:
        if re.search(pattern, query_upper):
            return False, f"Operación no permitida: {pattern}"
    
    if not re.match(r'^\s*(SELECT|WITH|EXPLAIN)', query_upper):
        return False, "Solo se permiten consultas SELECT, WITH o EXPLAIN"
    
    return True, ""


def convert_value(val):
    if isinstance(val, Decimal):
        return float(val)
    return val

def execute_safe_query(query: str) -> dict:
    valid, error = validate_query(query)
    if not valid:
        return {"success": False, "error": error}
    
    conn = None
    try:
        conn = get_sync_conn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ({query}) as subquery")
            total_count = int(cur.fetchone()[0])
            
            if total_count > MAX_ROWS:
                return {
                    "success": False, 
                    "error": f"Límite excedido: {total_count} filas. Máximo permitido: {MAX_ROWS}"
                }
            
            safe_query = f"""
                SELECT * FROM ({query}) as subquery 
                LIMIT {MAX_ROWS}
            """
            cur.execute(safe_query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            results = [{k: convert_value(v) for k, v in dict(zip(columns, row)).items()} for row in rows]
            
            return {
                "success": True,
                "columns": columns,
                "rows": results,
                "total": total_count,
                "returned": len(results)
            }
            
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            release_sync_conn(conn)


def get_sucursales() -> list:
    conn = None
    try:
        conn = get_sync_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT num_sucursal, sucursal 
                FROM ventas_items 
                WHERE num_sucursal IS NOT NULL
                ORDER BY num_sucursal
            """)
            return [{"num_sucursal": int(r[0]) if r[0] else None, "sucursal": r[1]} for r in cur.fetchall()]
    finally:
        if conn:
            release_sync_conn(conn)


def get_available_period() -> dict:
    conn = None
    try:
        conn = get_sync_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(fecha_captura), MAX(fecha_captura) 
                FROM ventas_items 
                WHERE fecha_captura IS NOT NULL
            """)
            result = cur.fetchone()
            return {
                "fecha_min": str(result[0]) if result[0] else None,
                "fecha_max": str(result[1]) if result[1] else None
            }
    finally:
        if conn:
            release_sync_conn(conn)


def predict_ventas_puertas(sucursal: Optional[str], meses: int = 3) -> dict:
    conn = None
    try:
        conn = get_sync_conn()
        
        where_clause = "WHERE descripcion ILIKE 'H-%'"
        if sucursal:
            where_clause += f" AND sucursal ILIKE '%{sucursal}%'"
        
        query = f"""
            SELECT 
                DATE_TRUNC('month', fecha)::date as mes,
                descripcion,
                SUM(cantidad) as cantidad
            FROM ventas_items
            {where_clause}
            AND fecha IS NOT NULL
            GROUP BY DATE_TRUNC('month', fecha)::date, descripcion
            ORDER BY mes, descripcion
        """
        
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        
        if not rows:
            return {"success": False, "error": "Sin datos históricos para puertas"}
        
        df = pd.DataFrame(rows, columns=["mes", "descripcion", "cantidad"])
        df["mes"] = pd.to_datetime(df["mes"])
        
        resultados = {}
        
        for puerta in df["descripcion"].unique():
            puerta_df = df[df["descripcion"] == puerta].copy()
            puerta_df = puerta_df.sort_values("mes")
            
            if len(puerta_df) < 3:
                continue
            
            puerta_df = puerta_df.set_index("mes")
            mensual = puerta_df["cantidad"].resample("ME").sum().fillna(0)
            mensual = mensual[mensual > 0]
            
            if len(mensual) < 3:
                continue
            
            valores = mensual.values
            n = len(valores)
            
            mejor_metodo = None
            mejor_error = float("inf")
            predicciones = []
            
            # 1. Regresión lineal
            x = np.arange(len(valores))
            coef = np.polyfit(x, valores, 1)
            pred_lr = [coef[0] * (n + i) + coef[1] for i in range(1, meses + 1)]
            error_lr = np.mean(np.abs(valores[-3:])) if n >= 3 else np.mean(valores)
            
            if error_lr < mejor_error:
                mejor_error = error_lr
                mejor_metodo = "regresion_lineal"
                predicciones = pred_lr
            
            # 2. Media móvil (últimos 3 meses)
            if n >= 3:
                media_movil = np.mean(valores[-3:])
                pred_ma = [media_movil] * meses
                error_ma = np.mean(np.abs(valores[-3:] - media_movil))
                
                if error_ma < mejor_error:
                    mejor_error = error_ma
                    mejor_metodo = "media_movil"
                    predicciones = pred_ma
            
            # 3. Estacional simple (mismo mes del año anterior)
            if n >= 12:
                ultimos_12 = valores[-12:]
                estacional = ultimos_12[-meses:] if len(ultimos_12) >= meses else ultimos_12
                pred_est = list(estacional) + [np.mean(ultimos_12)] * (meses - len(estacional)) if len(ultimos_12) < meses else estacional
                error_est = np.mean(np.abs(valores[-3:] - np.mean(valores[-3:])))
                
                if error_est < mejor_error:
                    mejor_error = error_est
                    mejor_metodo = "estacional"
                    predicciones = pred_est
            
            ultimo_mes = mensual.index[-1]
            preds = []
            for i in range(meses):
                mes_pred = ultimo_mes + pd.DateOffset(months=i + 1)
                preds.append({
                    "mes": mes_pred.strftime("%Y-%m"),
                    "cantidad": int(round(predicciones[i])),
                    "metodo": mejor_metodo
                })
            
            resultados[puerta] = {
                "historial": [{"mes": m.strftime("%Y-%m"), "cantidad": int(c)} for m, c in mensual.items()],
                "predicciones": preds,
                "metodo_usado": mejor_metodo
            }
        
        if not resultados:
            return {"success": False, "error": "No hay suficientes datos para predecir"}
        
        return {
            "success": True,
            "sucursal": sucursal or "total",
            "meses_predecidos": meses,
            "predicciones_por_puerta": resultados
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            release_sync_conn(conn)


MCP_TOOLS = {
    "query_ventas": {
        "description": """Ejecuta consultas SQL SELECT en la tabla de ventas.
        Útil para: análisis de ventas, reportes por sucursal, categorías populares, 
        tendencias por fecha, filtros por cliente, vendedor, método de pago, etc.
        
        La tabla se llama 'ventas_items' y contiene todas las transacciones de venta.
        Puedes usar: SELECT, WITH, EXPLAIN. No se permiten modificaciones.""",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Consulta SQL SELECT. Ejemplo: SELECT sucursal, SUM(precio_final) as total FROM ventas_items WHERE fecha >= '2025-01-01' GROUP BY sucursal ORDER BY total DESC"
                }
            },
            "required": ["query"]
        }
    },
    "get_schema": {
        "description": "Retorna el esquema de la tabla de ventas con descripción de cada columna.",
        "inputSchema": {"type": "object", "properties": {}}
    },
    "get_sucursales": {
        "description": "Lista todas las sucursales disponibles con su número y nombre.",
        "inputSchema": {"type": "object", "properties": {}}
    },
    "get_available_period": {
        "description": "Retorna el rango de fechas con datos disponibles.",
        "inputSchema": {"type": "object", "properties": {}}
    },
    "predict_puertas": {
        "description": """Predice ventas de puertas (códigos H-%) para N meses.
        Analiza automáticamente el mejor método (regresión lineal, media móvil o estacional).
        Returns: desglose por puerta/modelo con historial y predicciones.""",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sucursal": {
                    "type": "string",
                    "description": "Nombre de sucursal (ej: Altamisa). Omitir para total."
                },
                "meses": {
                    "type": "integer",
                    "description": "Cantidad de meses a predecir (default: 3)"
                }
            }
        }
    },
}


@bp.route("/mcp", methods=["POST"])
def mcp_endpoint():
    try:
        body = request.get_json()
        
        if not body:
            return jsonify({"error": "Invalid request body"}), 400
        
        method = body.get("method")
        params = body.get("params", {})
        
        if method == "tools/list":
            tools = []
            for name, tool in MCP_TOOLS.items():
                tools.append({
                    "name": name,
                    "description": tool["description"],
                    "inputSchema": tool["inputSchema"]
                })
            return jsonify({"result": {"tools": tools}})
        
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name == "query_ventas":
                query = arguments.get("query", "")
                result = execute_safe_query(query)
                return jsonify({
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(result, default=str, ensure_ascii=False)
                        }]
                    }
                })
            
            elif tool_name == "get_schema":
                return jsonify({
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(VENTAS_SCHEMA, ensure_ascii=False, indent=2)
                        }]
                    }
                })
            
            elif tool_name == "get_sucursales":
                sucursales = get_sucursales()
                return jsonify({
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(sucursales, ensure_ascii=False)
                        }]
                    }
                })
            
            elif tool_name == "get_available_period":
                period = get_available_period()
                return jsonify({
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(period, ensure_ascii=False)
                        }]
                    }
                })
            
            elif tool_name == "predict_puertas":
                sucursal = arguments.get("sucursal")
                meses = arguments.get("meses", 3)
                result = predict_ventas_puertas(sucursal, meses)
                return jsonify({
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": json.dumps(result, ensure_ascii=False, default=str)
                        }]
                    }
                })
            
            else:
                return jsonify({"error": f"Unknown tool: {tool_name}"}), 400
        
        else:
            return jsonify({"error": f"Unknown method: {method}"}), 400
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/mcp", methods=["GET"])
def mcp_info():
    return jsonify({
        "name": "reportecloud-mcp",
        "version": "1.0.0",
        "description": "MCP Server para consultas de ventas",
        "tools": list(MCP_TOOLS.keys())
    })


@bp.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "mcp"})


@bp.route("/mcp/query", methods=["POST"])
def query_ventas():
    try:
        body = request.get_json()
        query = body.get("query", "") if body else ""
        
        if not query:
            return jsonify({"error": "Falta query"}), 400
        
        result = execute_safe_query(query)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/mcp/schema", methods=["GET"])
def get_schema():
    return jsonify(VENTAS_SCHEMA)


@bp.route("/mcp/sucursales", methods=["GET"])
def get_sucursales_endpoint():
    return jsonify(get_sucursales())


@bp.route("/mcp/periodo", methods=["GET"])
def get_periodo():
    return jsonify(get_available_period())
