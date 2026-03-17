# wdfw

# 🎣 WA Fishing Regulations AI App (FREE)

## 📌 Overview
The **WA Fishing Regulations AI App** is a free, AI-powered assistant that helps Washington State anglers understand fishing rules and stay compliant.

It continuously reads data from the **Washington Department of Fish and Wildlife (WDFW)** website and enables users to:

- Ask natural language questions about fishing regulations  
- Get accurate, up-to-date answers  
- Receive alerts when emergency rules change  

---

## 💬 Example Query

```
What is the size and bag limit for Chinook salmon in marine area 10-1 after October 15th?
```

---

## 🎯 Problem

Fishing regulations in Washington State are:

- Complex and difficult to read  
- Spread across multiple categories:
  - Statewide rules  
  - Freshwater regulations  
  - Marine area regulations  
  - Species-specific limits  
- Frequently updated via emergency rules  

Missing a rule or update can result in **significant fines**.

---

## ✅ Solution

This app transforms WDFW regulations into a **queryable AI system** that:

- Converts unstructured web pages into structured data  
- Tracks changes in near real-time  
- Provides simple, human-readable answers  
- Alerts users when emergency rules are issued  

---

## 🚀 Impact

### 📊 Reach

| Metric | Value |
|------|------|
| Total anglers (2022) | 1.2 million |
| Fishing licenses sold (2025) | ~866,000 |
| Commercial crab license holders | 228 |
| Commercial fishing vessels | ~400 |

---

### 💡 Value

| User Type | Benefit |
|----------|--------|
| Recreational Fishers | Easy access to rules |
| Commercial Fishers | Avoid costly violations |
| General Public | Free AI-powered tool |
| Regulators | Better compliance |

---

### ⚠️ Problems Solved

| Challenge | Solution |
|----------|--------|
| Hard-to-read regulations | Natural language interface |
| Frequent updates | Real-time monitoring |
| Risk of fines | Accurate, current data |
| Fragmented sources | Unified system |

---

## 🧠 Architecture

The system is built as a **streaming data pipeline + AI query layer**.

### 🔧 Components

| Component | Description |
|----------|------------|
| Python App | Core application logic |
| Kafka Producer | Streams raw regulation data |
| Avro Schemas (2) | Data serialization |
| Apache Flink | Processes and extracts rules |
| Confluent S3 Sink Connector | Stores processed data |
| WDFW Website | Source of truth |

---

## 🔄 Data Flow

```
WDFW Website
     ↓
Python Scraper
     ↓
Kafka Producer (Serialized with Avro)
     ↓
Apache Flink Processing
     ↓
Structured Rules + Change Detection
     ↓
S3 (via Confluent Connector)
     ↓
AI Query Interface
     ↓
User Responses + Alerts
```

---

## ⚡ Features

| Feature | Description |
|--------|------------|
| Natural Language Q&A | Ask questions like a human |
| Real-Time Updates | Detects emergency rule changes |
| Alerts | Notify users of critical updates |
| Structured Data | Extracts usable regulation data |
| Free Access | Available to all WA residents |

---

## 🐟 Example Use Cases

| Scenario | Result |
|---------|--------|
| Checking salmon limits | Instant answer |
| Planning a trip | Up-to-date rules |
| Commercial compliance | Avoid penalties |
| Emergency closure | Immediate notification |

---

## 🛠️ Tech Stack

| Layer | Technology |
|------|-----------|
| Language | Python |
| Streaming | Apache Kafka |
| Processing | Apache Flink |
| Storage | AWS S3 |
| Serialization | Avro |
| Connectors | Confluent S3 Sink |
| Data Source | WDFW Website |

---

## 📦 Getting Started

### Prerequisites

- Python 3.9+
- Kafka cluster (Confluent Cloud or local)
- Schema Registry
- Apache Flink (optional for processing layer)
- AWS S3 bucket
- Confluent S3 Sink Connector

---

### Installation

```bash
git clone https://github.com/your-repo/wa-fishing-ai-app.git
cd wa-fishing-ai-app

pip install -r requirements.txt
```

---

### Configuration

Set environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=<your-kafka-endpoint>
export SCHEMA_REGISTRY_URL=<your-schema-registry>
export S3_BUCKET_NAME=<your-bucket>
```

---

### Run the App

```bash
python app.py
```

---

## 🔔 Future Enhancements

- 📱 Mobile app interface  
- 📍 Location-based regulation filtering  
- 🔔 Push notifications  
- 🦌 Expansion to hunting regulations  
- 🧠 Improved AI query accuracy  

---

## 🌎 Why This Matters

This project demonstrates a **real-world AI application** that:

- Helps hundreds of thousands of people  
- Makes government data accessible  
- Improves compliance and safety  
- Saves time and money  

---

## 📄 License

This project is free to use and intended for public benefit.
