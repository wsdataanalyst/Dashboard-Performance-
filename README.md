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
- **Obrigatório**: preencha também `ADMIN_USERNAME` e `ADMIN_PASSWORD` (o app não inicia sem isso)

**Opção B: .env**

- Copie `.env.example` para `.env`
- Preencha `GOOGLE_API_KEY` e/ou `OPENAI_API_KEY`
- **Obrigatório**: preencha também `ADMIN_USERNAME` e `ADMIN_PASSWORD` (o app não inicia sem isso)

5) Rode o app:

```powershell
streamlit run streamlit_app.py
```

### Observações importantes

- **Histórico** fica em `data\app.db` (SQLite).
- **Uploads** ficam em `data\uploads\` (salvos por análise para auditoria).
- Para publicar no Streamlit Community Cloud, use `streamlit_app.py` como arquivo principal e cadastre suas chaves em **Secrets**.

### Automação (importar pasta → salvar no histórico)

Se você quiser que o sistema busque planilhas de uma pasta e salve automaticamente no histórico (sem precisar abrir o app),
use o script `scripts/auto_import.py`.

Ele lê arquivos `*.xls*`/`*.xlsx` de uma pasta (inbox), roda a mesma importação do app e salva no histórico, incluindo auditoria
dos anexos em `data/uploads/`.

Variáveis de ambiente (opcionais):

- `AUTO_IMPORT_DIR`: pasta de entrada (default: `auto_inbox/` na raiz do projeto)
- `AUTO_IMPORT_ARCHIVE_DIR`: pasta para arquivar os arquivos processados (default: `auto_archive/`)
- `AUTO_IMPORT_PERIODO`: período a salvar (default: data do dia, ex. `Até 26/04/2026`)
- `AUTO_IMPORT_OWNER_USERNAME`: usuário dono da análise (se vazio, salva como **público**: `owner_user_id = NULL`, visível a todos os usuários logados)

Exemplo (Windows / PowerShell):

```powershell
$env:AUTO_IMPORT_DIR = "C:\Caminho\Para\Sua\Pasta"
$env:AUTO_IMPORT_PERIODO = "Abril/2026"
.\.venv\Scripts\python.exe scripts\auto_import.py
```

