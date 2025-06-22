from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://prefect:prefect@postgres:5432/prefect"

try:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        print("✅ Conexão bem-sucedida!")
        # Corrigindo a execução da query com text()
        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
        print("Tabelas existentes:", result.fetchall())
except Exception as e:
    print(f"❌ Falha na conexão: {e}")