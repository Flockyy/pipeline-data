# 🧩 Migration vers DLT (Data Load Tool)

## 🎯 Objectif
Remplacer les anciens scripts Python (`download_data.py`, `import_to_postgres.py`, `data_cleaner.py`) par une pipeline unifiée DLT capable de télécharger, nettoyer et charger les données dans PostgreSQL.

---

## ⚙️ Installation et configuration

### 1. Installation
```bash
pip install dlt[postgres,parquet]==0.4.3
