# 🚀 Big Data Project – Scalable Sampling & Partitioning for Imbalanced Data

This repository contains the implementation of my Big Data project:
**“From Imbalance to Insight: Scalable Sampling and Partitioning Strategies for Predicting Hospital Readmissions in Diabetic Patients.”**

The project focuses on designing **scalable oversampling and partitioning methods** in **Apache Spark** to handle severe class imbalance in healthcare datasets.

---

## 📂 Repository Structure

```bash
.
├── bigdata_v1.ipynb    # Main implementation (preprocessing, sampling, partitioning, modeling, evaluation)
├── eda.ipynb           # Exploratory Data Analysis notebook
├── data/               # Dataset (UCI Diabetes 130-US Hospitals)
├── results/            # Metrics, plots, confusion matrices, ROC curves
└── README.md
```

---

## 🧠 Project Overview

* **Objective:** Predict 30-day hospital readmissions for diabetic patients in a **highly imbalanced dataset**.
* **Challenge:** Only a small fraction of patients are readmitted → standard ML models fail to detect minority cases.
* **Approach:** Built a scalable pipeline in **PySpark** with **custom oversampling + hybrid partitioning** strategies to balance the data and optimize distributed training.
* **Outcome:** Achieved an **AUC of 0.875** with **Adaptive Borderline-SMOTE-BD** + **custom hybrid partitioning**, while keeping training time under **4 minutes**.

---

## 🔧 Techniques Used

### **Sampling Strategies**

* **Undersampling:** Random and stratified undersampling by age band.
* **Oversampling:**

  * **SMOTE-NC:** Baseline for mixed numerical/categorical features.
  * **Custom Distributed SMOTE-BD Variants:** Implemented directly in Spark.

    * **Standard SMOTE-BD:** Partition-wise k-NN interpolation for minority synthesis.
    * **Borderline-SMOTE-BD:** Focused on borderline minority cases near majority clusters.
    * **Adaptive Borderline-SMOTE-BD (Best):** Dynamically adjusted the number of synthetic samples based on density of majority neighbors, improving boundary learning.
    * **Exact Borderline-SMOTE-BD:** Generated exactly enough samples to perfectly balance minority vs majority.

### **Partitioning Strategies**

* **Random Partitioning:** Default Spark repartitioning.
* **Custom Hybrid Partitioning (Proposed):** Grouped patients using bucketed clinical features:

  * *Time in hospital (1–14, bucket=5)*
  * *Number of medications (0–80, bucket=5)*
  * *Number of diagnoses (1–16, bucket=2)*
    → Created composite partition keys (e.g., `1_3_4`) for improved data locality and reduced shuffle.

### **Modeling**

* Gradient Boosted Trees (GBTClassifier, Spark MLlib).
* Preprocessing with **VectorAssembler + StringIndexer** for categorical features.

### **Performance Tuning**

* Broadcast variables for neighbor models.
* Repartition tuning (optimal at 2–4 partitions).
* Shuffle reduction via hybrid strategy.

---

## 📊 Results

* **Custom Hybrid Partitioning** consistently reduced training time vs. Spark’s random partitioning (↓ 30–40%).
* **SMOTE-NC**: Best AUC (0.884) but longest runtime (12+ min).
* **Adaptive Borderline-SMOTE-BD**: Best trade-off → **AUC = 0.875**, training ≈ 3.8 min.
* **GBTClassifier** proved effective for non-linear healthcare data while remaining explainable.

📈 *Example: Performance with 4 hybrid partitions*

| Sampling Method                  | Training Time (s) |       AUC |
| -------------------------------- | ----------------: | --------: |
| SMOTE-NC                         |             758.7 |     0.884 |
| SMOTE-BD                         |             455.4 |     0.852 |
| Borderline-SMOTE-BD              |             302.1 |     0.869 |
| **Adaptive Borderline-SMOTE-BD** |         **375.6** | **0.875** |
| Exact Borderline-SMOTE-BD        |             661.1 |     0.836 |

---

## ⚙️ Tech Stack

* **Frameworks:** Apache Spark (PySpark), Spark SQL, Spark MLlib
* **Languages:** Python
* **Libraries:** NumPy, Pandas, Seaborn, Matplotlib
* **Environment:** Jupyter Notebook, Google Colab, UoN HPC Cluster

---

## 🚀 Getting Started

```bash
# Clone repo
git clone https://github.com/abuqaiselegant/BigData.git
cd bigdata-project

# Install dependencies
pip install pyspark pandas seaborn matplotlib
```

Run notebooks:

* `eda.ipynb` → Data exploration & visualization
* `bigdata_v1.ipynb` → Full scalable pipeline

---

## 📌 Future Work

* Extend to **ADASYN** and **SMOTETomek**.
* Scale experiments on **>1M records** using HPC cluster.
* Integrate with **real-time Spark Structured Streaming** for clinical decision support.
* Explore **explainable ML (GBT-HIPS, SHAP)** for healthcare interpretability.

---

## 🏷️ Citation

If you use this work, please cite:

> Qais, Abu et al. *From Imbalance to Insight: Scalable Sampling and Partitioning Strategies for Predicting Hospital Readmissions in Diabetic Patients.* University of Nottingham, 2025.

---
