# IEWC Ingestor – Azure Function App

This project is an **Azure Function App** designed to extract and ingest data from **[https://www.iewc.com/resources](https://www.iewc.com/resources)**.
The function retrieves **HTML**, **PDF**, and **TXT** content, processes it, and stores the extracted information into a **Qdrant vector database**.

---

## 🐍 Python Version

This project uses **Python 3.11**. Ensure your environment is created using Python 3.11 for compatibility.

---

## 🔧 Prerequisites

Before running this project, ensure you have:

* **Python 3.11** installed
* **Azure Functions Core Tools** (v4)
* **Azure CLI** installed and logged in
* **Qdrant instance** (local or cloud)
* **Environment variables** configured:

  * `QDRANT_URL`
  * `QDRANT_API_KEY`
  * `OPENAI_API_KEY`
  * Any additional keys required for your data source

---

## 🚀 Features

* Scrapes and extracts data from IEWC Resources webpage
* Supports **HTML**, **PDF**, and **TXT** content processing
* Generates vector embeddings and inserts data into **Qdrant**
* Built using **Python Azure Functions**
* Easily deployable to Azure Function App

---

## 📦 Setup Instructions

### 1. **Create Python Virtual Environment**

Using Conda:

```bash
conda create -n iewc-ingestor python=3.11
```

OR using `venv`:

```bash
python3.11 -m venv .venv
```

---

### 2. **Activate Virtual Environment**

```bash
.venv\Scripts\activate
```

To deactivate:

```bash
deactivate
```

---

### 3. **Install Required Libraries**

```bash
pip install -r .\requirements.txt
```

---

### 4. **Run Function App Locally**

Start and test the Azure Function locally:

```bash
func start
```

---

### 5. **Login to Azure**

```bash
az login
```

---

### 6. **Deploy the Azure Function to Azure**

Deploy to the Function App named **iewc-ingestor**:

```bash
func azure functionapp publish iewc-ingestor --python
```

---

## 📘 Description

This Azure Function App collects data from:

🔗 **[https://www.iewc.com/resources](https://www.iewc.com/resources)**

It extracts:

* HTML content
* PDF files
* TXT content

After extraction and processing, the cleaned data is stored inside a **Qdrant vector database** for semantic search and AI-powered applications.
