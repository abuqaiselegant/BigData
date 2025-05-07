# BigData

An end-to-end PySpark pipeline for predicting 30-day hospital readmissions using the U.S. Diabetes 130-Yearly Readmissions dataset. This project covers:

- **Data Ingestion & EDA**: Load and profile the US Diabetes 130 dataset with schema inference, missing-value analysis, and feature summaries.  
- **Feature Engineering**: Index and one-hot encode categorical variables, normalize numeric features, and assemble everything into Spark ML feature vectors.  
- **Class Balancing**: Implement and compare multiple balancing strategies—random undersampling and several SMOTE-BD variants (standard, borderline, adaptive, exact)—to address severe class imbalance.  
- **Modeling & Evaluation**: Train a Gradient-Boosted Trees classifier in Spark MLlib and evaluate with confusion matrices, ROC/AUC curves, and classification reports from scikit-learn.  
- **Reproducible Workflow**: Parameterized PySpark scripts and configurations make it easy to rerun experiments or adapt the pipeline to other healthcare classification tasks.  
