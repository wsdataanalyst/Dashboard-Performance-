## Dashboard Performance (Streamlit)

Projeto **novo do zero** para analisar performance/bônus a partir de **prints**:

- Upload de 1–5 imagens (prints)
- Extração de dados por IA com **fallback automático**: **Gemini → OpenAI** (ou o inverso)
- Dashboard moderno (tabela + gráficos + indicadores)
- **Histórico consistente** em SQLite (sem perder análises)
- Segurança: chaves via **Streamlit Secrets** ou `.env` (nunca no código)

### Como rodar (Windows / PowerShell)

1) Abra esta pasta no VS Code.

2) Crie e ative um ambiente virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
```

3) Instale as dependências:

```powershell
pip install -r requirements.txt
```

4) Configure as chaves (escolha **uma** forma)

**Opção A (recomendada): Streamlit Secrets**

- Copie `.streamlit\secrets.toml.example` para `.streamlit\secrets.toml`
- Preencha `GOOGLE_API_KEY` e/ou `OPENAI_API_KEY`

**Opção B: .env**

- Copie `.env.example` para `.env`
- Preencha `GOOGLE_API_KEY` e/ou `OPENAI_API_KEY`

5) Rode o app:

```powershell
streamlit run streamlit_app.py
```

### Observações importantes

- **Histórico** fica em `data\app.db` (SQLite).
- **Uploads** ficam em `data\uploads\` (salvos por análise para auditoria).
- Para publicar no Streamlit Community Cloud, use `streamlit_app.py` como arquivo principal e cadastre suas chaves em **Secrets**.

