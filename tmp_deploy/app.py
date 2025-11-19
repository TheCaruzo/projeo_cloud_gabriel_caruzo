from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys

# Make `process/` importable. Add the repository root to `sys.path`
# so `import process.queries` will work whether running from the
# repo root or from this backend directory.
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Now import the queries module from the `process` package/directory
from process.queries import select_all_pregao, insert_pregao_bulk



app = Flask(__name__)

# Configure CORS using an environment variable so we can control allowed origins
# without changing code. Set `FRONTEND_ORIGINS` to a comma-separated list of
# allowed origins (example: "https://example.com,https://app.example.com"). If
# the variable is not set, default to allowing all origins (useful for dev).
front_origins = os.environ.get("FRONTEND_ORIGINS", "")
if front_origins:
    origins = [o.strip() for o in front_origins.split(",") if o.strip()]
    CORS(app, origins=origins)
else:
    CORS(app)


def row_to_dict(row):
    # Expecting row ordinal: Id, Ativo, DataPregao, Abertura, Fechamento, Volume
    abertura = row[3] if len(row) > 3 else None
    fechamento = row[4] if len(row) > 4 else None
    preco_medio = None
    try:
        if abertura is not None and fechamento is not None:
            preco_medio = float((float(abertura) + float(fechamento)) / 2)
    except Exception:
        preco_medio = None

    return {
        "id": row[0],
        "nome": row[1],
        "dataPregao": str(row[2]) if row[2] is not None else None,
        "precoAbertura": float(abertura) if abertura is not None else None,
        "precoFechamento": float(fechamento) if fechamento is not None else None,
        "volumeDiario": float(row[5]) if len(row) > 5 and row[5] is not None else None,
        "precoMedio": preco_medio,
    }


@app.route("/assets", methods=["GET"])
def list_assets():
    """List assets (basic). Optional query param `limit` to limit results."""
    try:
        rows = select_all_pregao()
        limit = request.args.get("limit", None)
        if limit:
            try:
                limit = int(limit)
                rows = rows[:limit]
            except Exception:
                pass
        data = [row_to_dict(r) for r in rows]
        return jsonify({"count": len(data), "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ingest", methods=["POST"])
def ingest():
    """Recebe uma lista JSON de registros e insere em bulk na tabela Cotacoes.

    Payload esperado: JSON array de objetos com chaves: Ativo, DataPregao, Abertura, Fechamento, Volume
    """
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "payload vazio"}), 400

        # insert_pregao_bulk aceita lista de dicts ou DataFrame
        inserted = insert_pregao_bulk(payload, table_name="Cotacoes")
        return jsonify({"inserted": inserted}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def root():
    """Simple root for health check and quick info."""
    return jsonify({
        "status": "ok",
        "endpoints": ["/assets"],
        "note": "Use /assets to fetch data. If you see 404 on /, call /assets instead."
    })


if __name__ == "__main__":
    try:
        import process.queries as _q
        queries_path = getattr(_q, '__file__', None)
    except Exception:
        queries_path = None

    print(f"Starting backend (pid={os.getpid()})")
    print(f"Python executable: {sys.executable}")
    print(f"Repo root added to sys.path: {repo_root}")
    print(f"Imported queries module path: {queries_path}")

    app.run(host="0.0.0.0", port=8000, debug=True)
