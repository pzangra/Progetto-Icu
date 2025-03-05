ICU Mortality Prediction for Sepsis/SIRS Patients

Authors: Pietro Maria Zangrando, Riccardo Sanna, Emma Torracca
Date: [Today's Date]
Overview

This repository contains the code and documentation for our Bachelor Thesis project in Biomedical Engineering. Our work focuses on developing machine learning models to predict mortality in ICU patients diagnosed with Sepsis or Systemic Inflammatory Response Syndrome (SIRS) using the publicly available MIMIC-IV database. In addition to the predictive models, the project investigates potential socio-economic bias in the predictions. We employ several models including Logistic Regression, XGBoost, and LightGBM, and apply hyperparameter tuning techniques (Random Search, Grid Search, and Optuna) to optimize performance.
Repository Structure

    THESIS_PZ.pdf
    The complete thesis document, which details the clinical background, methodology, data extraction, preprocessing, and results.

    mimiciv_3_9.ipynb
    A Jupyter Notebook that includes:
        Data extraction from the MIMIC-IV database,
        Data cleaning and preprocessing (using KNN imputation, outlier management, and encoding),
        Model training and evaluation using metrics such as Accuracy, AUROC, F1-score, and MCC,
        Hyperparameter tuning and SHAP analysis for model interpretability.

    query.sql
    SQL code to create the starting dataset of patients and features to analyze with machine learning models and predict on after a train/test. 

    requirements.txt
    A list of required Python packages, including scikit-learn, xgboost, lightgbm, optuna, and shap.

How to Run the Code

Follow these steps to reproduce the experiments and view the results:

    Clone the Repository:

git clone https://github.com/pzangra/Progetto-Icu.git
cd ICU-Mortality-Prediction

Install Dependencies:

pip install -r requirements.txt

Run the Jupyter Notebook:

    jupyter notebook mimiciv_3_9.ipynb

Key Project Details

    Data Extraction and Preprocessing:
    We extract clinical and demographic data from the MIMIC-IV database for patients with Sepsis/SIRS. Data cleaning includes handling missing values via KNN imputation and managing outliers with IQR-based techniques.

    Feature Engineering:
    Clinical scores like SIRS and SOFA are calculated. We also select key variables (vital signs and laboratory test results) as input features for our models.

    Model Training and Evaluation:
    We compare a baseline Logistic Regression model (which offers good interpretability) with tree-based methods such as XGBoost and LightGBM. Hyperparameter tuning is conducted using RandomSearchCV, Optuna, and GridSearch. In addition, we use SHAP analysis to interpret model outputs.

    Bias Analysis:
    The study investigates socio-economic bias by forcing demographic variables and comparing model performance to ensure robust predictions.

Project Results

Our experiments show that the tree-based models (XGBoost and LightGBM) achieve competitive performance, with AUC-ROC scores ranging from approximately 0.84 to 0.86. Although Logistic Regression is less sensitive, its linear coefficients offer greater interpretability. Analysis with SHAP reveals that key variables—such as SOFA score, heart rate, mean arterial pressure, lactate levels, and white blood cell count—are critical in predicting mortality.
Project Workflow, based on the studies of Wang (2024, https://doi.org/10.1007/s11739-024-03732-2) and Mollura (2024, https://doi.org/10.1371/journal.pdig.0000459) which respectively create a model using LM and XGB for prediction of mortality and for diagnosis of septic patients.

The project follows a structured workflow:

    Data Extraction: SQL queries on the MIMIC-IV database.
    Data Preprocessing: Cleaning data, imputing missing values, and removing outliers.
    Feature Engineering: Calculating clinical scores and selecting relevant variables.
    Model Training & Evaluation: Using different machine learning models with hyperparameter tuning.
    Interpretability and Bias Analysis: Employing SHAP analysis and testing for socio-economic bias.

Refer to the following images for a visual summary:

    Complete Workflow: See complete_process.png
    Hyperparameter Optimization: See optimization_hyp.png
    Patient Selection Process: See selection_process.jpeg

Contact Information

For further details or questions regarding this project, please contact:

    Pietro Maria Zangrando
    Email: piezangrando@gmail.com
    Riccardo Sanna
    Emma Torracca

Happy exploring! We hope this project serves as a useful resource for applying machine learning in clinical settings.
