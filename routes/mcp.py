import os
import re
import json
from functools import wraps

import httpx
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


def execute_safe_query(query: str) -> dict:
    valid, error = validate_query(query)
    if not valid:
        return {"success": False, "error": error}
    
    conn = None
    try:
        conn = get_sync_conn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ({query}) as subquery")
            total_count = cur.fetchone()[0]
            
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
            
            results = [dict(zip(columns, row)) for row in rows]
            
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
            return [{"num_sucursal": r[0], "sucursal": r[1]} for r in cur.fetchall()]
    finally:
        if conn:
            release_sync_conn(conn)


def get_available_period() -> dict:
    conn = None
    try:
        conn = get_sync_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(fecha), MAX(fecha) 
                FROM ventas_items 
                WHERE fecha IS NOT NULL
            """)
            result = cur.fetchone()
            return {
                "fecha_min": str(result[0]) if result[0] else None,
                "fecha_max": str(result[1]) if result[1] else None
            }
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
