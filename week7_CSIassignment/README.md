
<h1 align="center"> Week 7: Truncate and Load ADF Pipeline </h1>

<p align="center">
  <img src="./images/Pasted%20image%2020250706181648.png" alt="Pipeline Showcase" width="700"/>
</p>

## 📌 Project Overview

This week's assignment focused on building a **Truncate and Load pipeline using Azure Data Factory (ADF)** that executes on a **daily schedule**. The task required handling and processing three distinct types of `.csv` files stored in **Azure Data Lake Storage Gen2 (ADLS)**, involving dynamic metadata-driven design and conditional branching based on file structure.

### Key Concepts Practiced:

- Working with metadata activities
- Creating parameterized data flows
- Parsing file names for dynamic loading
- Implementing scheduled pipeline triggers
- Auto-extracting date components from filenames


## 🧠 Problem Statement

The solution involved dealing with the following three `.csv` file types:

| File Type               | Required Processing Logic                                                              |
| ----------------------- | -------------------------------------------------------------------------------------- |
| `CUST_MSTR_*.csv`       | Extract `file_date` → Convert `20191112` to `2019-11-12` → Load into `CUST_MSTR` table |
| `master_child_export-*` | Extract `file_date` and `date_key` from name → Load into `master_child` table          |
| `H_ECOM_ORDER.csv`      | Load the data as-is into `H_ECOM_Orders` table                                         |

📝 **Note**: Each table should be truncated before loading, and the pipeline is designed to run once per day.

> 🙏 Thanks to CSI (Celebal Technologies) for the opportunity to apply these concepts during the internship.

---

## 📂 Azure Resources Involved

- Azure Data Factory (ADF)
- Azure SQL Database + SQL Server
- Azure Data Lake Storage Gen2

<p align="center">
  <img src="./images/Pasted%20image%2020250706191351.png" width="700"/>
</p>


## 🔗 Linked Services

Set up connections for:

* Azure SQL DB
* Azure Data Lake Gen2

<p align="center">
  <img src="./images/LS.png" width="600"/>
</p>



## 🗂️ Datasets

Created datasets for:

- Reading input CSV files from ADLS
- Writing into SQL sink tables

<p align="center">
  <img src="./images/DS.png" width="600"/>
</p>

---

## 🧪 Pipeline Architecture

<p align="center">
  <img src="./images/Pasted%20image%2020250706181648.png" width="700"/>
</p>

This is the master pipeline that processes and loads all 3 types of files into their respective SQL tables on a daily basis.



## 🔁 CUST\_MSTR Flow

### ✅ Metadata Activity

<p align="center"><img src="./images/metadata1.png" width="600"/></p>

📌 Dataset: `CUST_MSTR_CSV`

### ✅ ForEach + Data Flow

<p align="center"><img src="./images/fe1.png" width="600"/></p>

📌 Items:

```json
@activity('GetMetadata_CUST_MSTR').output.childItems
```

### ✅ Data Flow Logic

<p align="center"><img src="./images/Pasted%20image%2020250706181744.png" width="600"/></p>

* **Derived Column**: `file_date`

```plaintext
toDate(
  replace(
    split(split(byName('source_file_name'), '/')[3], '_')[3],
    '.csv',
    ''
  ),
  'yyyyMMdd'
)
```

<p align="center"><img src="./images/Pasted%20image%2020250706181733.png" width="600"/></p>


## 🔁 master\_child\_export Flow

### ✅ Metadata Activity

<p align="center"><img src="./images/metadata2.png" width="600"/></p>

📌 Dataset: `master_child.csv`

### ✅ ForEach + Data Flow

<p align="center"><img src="./images/fe2.png" width="600"/></p>

📌 Items:

```json
@activity('GetMetadata_master_child').output.childItems
```

### ✅ Data Flow Logic

<p align="center"><img src="./images/Pasted%20image%2020250706181920.png" width="600"/></p>

* **Derived Column**:

```plaintext
file_date: toDate(replace(split(split(byName('source_file_name'), '/')[3],'-')[2],'.csv',''),'yyyyMMdd')

date_key: replace(split(split(byName('source_file_name'), '/')[3],'-')[2],'.csv','')
```

<p align="center"><img src="./images/Pasted%20image%2020250706181949.png" width="600"/></p>


## 🔁 H\_ECOM\_ORDER Flow

### ✅ Metadata Activity

<p align="center"><img src="./images/metadata3.png" width="600"/></p>

📌 Dataset: `H_ECOM_ORDER.csv`

### ✅ ForEach + Data Flow

<p align="center"><img src="./images/fe3.png" width="600"/></p>

📌 Items:

```json
@activity('GetMetadata_H_ECOM_ORDER').output.childItems
```

### ✅ Data Flow Logic

<p align="center"><img src="./images/Pasted%20image%2020250706182039.png" width="600"/></p>

* No transformation — load as-is

<p align="center"><img src="./images/Pasted%20image%2020250706182135.png" width="600"/></p>


## ⏰ Trigger Configuration

This pipeline is triggered **daily** using a scheduled trigger.

<p align="center"><img src="./images/Pasted%20image%2020250706195730.png" width="600"/></p>
<p align="center"><img src="./images/Pasted%20image%2020250706193550.png" width="600"/></p>


## 🌟 Suggestions for Enhancement

- Configure alerts using Logic Apps for success/failure notifications
- Store file execution logs in an audit table with file name and row count
- Add global parameters and filter logic for full pipeline flexibility

---

## 💼 Author

**Mahiwal Vaishnav**\
Celebal Summer Internship 2025\
**Week 7 - Truncate and Load using Azure Data Factory**