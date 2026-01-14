from flask import Flask, render_template, request, send_file
import os
import uuid

from services.analise_recusas import processar

app = Flask(__name__)

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    resultado = None
    arquivo_saida = None

    if request.method == "POST":
        arquivos = {}
        for campo in ["mes1", "mes2", "mes3", "codigos"]:
            f = request.files[campo]
            nome = f"{uuid.uuid4()}_{f.filename}"
            caminho = os.path.join(UPLOAD_DIR, nome)
            f.save(caminho)
            arquivos[campo] = caminho

        df = processar(
            arquivos["mes1"],
            arquivos["mes2"],
            arquivos["mes3"],
            arquivos["codigos"]
        )

        nome_saida = f"resultado_{uuid.uuid4()}.csv"
        caminho_saida = os.path.join(OUTPUT_DIR, nome_saida)
        df.to_csv(caminho_saida, index=False, encoding="utf-8-sig")

        resultado = df.head(10).to_dict(orient="records")
        arquivo_saida = nome_saida

    return render_template(
        "index.html",
        resultado=resultado,
        arquivo_saida=arquivo_saida
    )

@app.route("/download/<nome>")
def download(nome):
    return send_file(os.path.join(OUTPUT_DIR, nome), as_attachment=True)

if __name__ == "__main__":
    pass

