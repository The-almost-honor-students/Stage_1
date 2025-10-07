# Stage 1 – Building the Data Layer

This repository contains **Stage 1** of the *Search Engine Project*, developed as part of the **Big Data** course at the University of Las Palmas de Gran Canaria (ULPGC).  
The objective of this stage is to design, implement, and benchmark a modular **data layer** following the **Hexagonal Architecture** pattern.  
This architecture integrates a **Data Lake**, a **Data Mart**, and an **Inverted Index** within a unified, scalable framework.

## System Components

The system consists of the following layers:

- **Data Lake** – Stores raw digital book files and their associated metadata.  
- **Data Mart** – Provides structured, query-optimized data storage.  
- **Inverted Index** – Enables efficient full-text search across documents.  
- **Control Layer** – Manages ingestion, indexing, and validation workflows.

The architecture adheres to the **Ports and Adapters (Hexagonal)** model, ensuring a clear separation of concerns:

- `application/` – Business logic and repository interfaces.  
- `infrastructure/` – Concrete database adapters (MongoDB, SQLite, PostgreSQL).  
- `domain/` – Core entities, such as `Book`.  
- `control/` – Pipeline orchestration and task scheduling.  
- `benchmark/` – Benchmarking and performance measurement scripts.

The project also applies the **Repository pattern** to decouple persistence logic from application behavior.  
This approach allows different database backends (MongoDB, SQLite, PostgreSQL) to be easily swapped or extended.

---

## Running MongoDB and Mongo Express with Docker

MongoDB and Mongo Express are deployed using **Docker Compose**, providing a reproducible environment for development and testing.

### 1. Start the containers
```bash
docker-compose up -d
```


This command will:
	•	Launch the MongoDB service on port 27017.
	•	Launch the Mongo Express web interface on port 8081.

2. Access the Mongo Express interface

Once the containers are running, open a web browser and navigate to:

http://localhost:8081

From here, you can inspect the bench_inverted database and view the inverted_index collection, which contains the indexed terms and their corresponding postings lists.

⸻

Python Environment Setup

Before running the pipeline or benchmark scripts, set up the Python environment as follows:

1. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies
   
```bash
pip install -r requirements.txt
```


⸻

Running the Control Pipeline

The control pipeline automates the ingestion and indexing process.
It periodically downloads books from Project Gutenberg, generates metadata, and indexes them into MongoDB.

Execute the following command from the project root:

python control/main.py

During execution, the pipeline will:
	•	Download a new book that has not yet been processed.
	•	Store its text files in the Data Lake directory (datalake/YYYYMMDD/HH/).
	•	Create the corresponding metadata record.
	•	Index the book in MongoDB for later querying.

To stop the scheduler, press:

CTRL + C


⸻

Running Benchmarks

Benchmark scripts are located inside the benchmark/ directory.
Each subfolder corresponds to a specific database engine or index strategy.

Example for MongoDB metadata benchmarking:

```bash
python benchmark/mongodb/metadata_benchmark.py
```


Example for Inverted Index benchmarking:

```bash
python benchmark/mongodb/inverted_index_benchmark.py
```


Contributors

## Contributors

| Member | Role | Contribution |
|:--|:--|:--|
| **Gisela Belmonte Cruz** | MongoDB Developer | Implemented metadata management and the inverted index using MongoDB; designed the control pipeline and coordinated benchmarking execution. |
| **Kaarlo Caballero Nillukka** | PostgreSQL Developer | Developed the PostgreSQL module, designed database schemas, and conducted performance analysis and optimization. |
| **Nerea Valido Calzada** | Monolithic File Researcher | Implemented the single-file inverted index prototype and analyzed its scalability and performance limitations. |
| **Ancor González Hernández** | SQLite and Project Structure Engineer | Built the SQLite benchmark module and established the overall project and folder structure for consistent data organization. |

⸻

License

This project was developed for educational purposes as part of the Big Data course at ULPGC.
All code is released under the MIT License, unless otherwise specified.

