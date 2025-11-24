Welcome to your dbt project! This repository contains a complete analytics engineering workflow following the **staging -> intermediate -> marts** pattern, including tests, documentation, and modeling best practices.

---

## 🚀 Getting Started

### 1. Install dependencies
Create and activate your Python virtual environment:
```bash
python3 -m venv dbt-venv
source dbt-venv/bin/activate
```

### 2. Configure your profile
Update your `~/.dbt/profiles.yml` with your BigQuery project, dataset, and credentials.

---

## ▶️ Running the Project

### Run models
```bash
dbt run
```

### Run tests
```bash
dbt test
```

### Build docs
```bash
dbt docs generate
dbt docs serve
```

---

## 📁 Project Structure
```
/models
  /staging        → Clean, typed, renamed raw tables
  /intermediate   → Business logic transformations
  /marts          → Final fact & dimension models
/snapshots        → Historical tracking
/tests            → Custom tests
/macros           → Reusable logic (e.g., amenity, bathroom utils)
```

---

## 🧪 Testing Strategy
- **Schema tests:** `not_null`, `unique`, `accepted_values`, `relationships`
- **Custom tests:** `dbt_utils.unique_combination_of_columns`
- **Freshness checks:** Validates source table recency

All generic tests use the updated `arguments:` format for dbt 1.11 compatibility.

---

## 📊 Marts
Key outputs include:
- `fct_listing_day` – Daily grain fact table combining listings, availability, pricing, and reservations

---

## 🧱 dbt Best Practices Followed
- Naming conventions: `stg_`, `int_`, `dim_`, `fct_`
- One model per file
- Documentation for every model + column
- Business logic only in intermediate models
- Incremental strategies for large fact tables

---

## 📚 Helpful Resources
- dbt Docs: https://docs.getdbt.com/
- dbt Discourse: https://discourse.getdbt.com/
- dbt Slack: https://community.getdbt.com/
- dbt Blog: https://blog.getdbt.com/

---

## 🤝 Contributing
1. Create a branch
2. Make your changes
3. Run `dbt build` before pushing
4. Submit a pull request

---

## ⭐ About This Project
This repo powers a full analytics pipeline for rental marketplace data, built to demonstrate modeling, testing, and semantic layer design aligned with modern analytics engineering standards.
