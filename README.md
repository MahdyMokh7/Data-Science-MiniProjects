# Data-Science-MiniProjects
Some ML/data science good quality projects that I have done.

This repository contains a collection of **Data Science mini projects** (CA0–CA5) completed as part of coursework and personal exploration. Each project demonstrates hands-on experience with **data analysis, statistical modeling, machine learning, deep learning, NLP, and computer vision**, along with practical workflow design and reproducible coding practices.

The projects are organized into **separate folders for each course assignment (CA0–CA5)**, reflecting a gradual progression from foundational Python and statistics to advanced deep learning and modern AI techniques.

---

## **Project Structure**

```
data-science-mini-projects/
├── CA0/  # Statistical Simulation & Data Analysis
├── CA1/  # Langevin Sampling & Tableau Storytelling
├── CA2/  # Real-Time Payment Data Pipeline (Kafka & Spark)
├── CA3/  # Machine Learning Challenges (Classification, Regression, Recommender Systems)
├── CA4/  # Deep Learning Projects (MLP, CNN, RNN/LSTM)
├── CA5/  # Modern NLP, Semantic Search, Image Segmentation
├── README.md
└── requirements.txt
```

Each CA folder contains:

* **Datasets or instructions to download them**
* **Python scripts / Jupyter notebooks** for preprocessing, modeling, and evaluation
* **Visualizations** (plots, dashboards, confusion matrices)
* **Documentation of pipeline steps, hyperparameters, and results**

---

## **Overview of Each Project**

### **CA0 – Statistical Simulation & Data Analysis (Python)**

**Focus:** Monte Carlo simulations, probability, CLT, confidence intervals, hypothesis testing.
**Tasks & Skills:**

* Simulated roulette bets and analyzed expected returns.
* Predicted 2016 U.S. election outcomes using polling data with confidence intervals.
* Conducted t-tests for drug safety analysis; interpreted p-values and significance.
* Visualized distributions, trends, and probabilities using Matplotlib/Seaborn.

**Tools:** Python, NumPy, Pandas, Matplotlib/Seaborn, SciPy

---

### **CA1 – Langevin Sampling & Tableau Storytelling**

**Focus:** Probabilistic sampling and interactive dashboards for data storytelling.
**Tasks & Skills:**

* Implemented Langevin dynamics to sample from multivariate distributions.
* Built Tableau dashboards analyzing Airbnb datasets with KPIs, filters, and interactive features.
* Applied data storytelling principles to communicate trends and insights effectively.

**Tools:** Python (NumPy, plotting), Tableau

---

### **CA2 – Real-Time Payment Data Pipeline (Kafka & Spark)**

**Focus:** Building a high-throughput, real-time data processing and analytics pipeline.
**Tasks & Skills:**

* Streamed transaction events via **Kafka**; validated and logged errors.
* Processed historical data with **PySpark**; analyzed commissions and transaction patterns.
* Implemented fraud detection rules and streamed alerts in real-time.
* Visualized trends and business insights using Python dashboards.

**Tools:** Kafka, PySpark / Spark Streaming, MongoDB, Python, Prometheus, Plotly

---

### **CA3 – Machine Learning Challenges**

**Focus:** Traditional ML for classification, regression, and recommender systems.
**Tasks & Skills:**

* Predicted cancer patient survival (classification), daily bike rentals (regression), and movie ratings (recommender system).
* Applied data preprocessing, feature engineering, scaling, encoding, and hyperparameter tuning.
* Evaluated models using metrics like Accuracy, F1, RMSE, MAE, R².

**Tools:** Python, Scikit-learn, Pandas, NumPy, XGBoost, LightGBM, CatBoost, Matplotlib/Seaborn

---

### **CA4 – Deep Learning Applications**

**Focus:** MLPs, CNNs, and RNN/LSTMs for real-world prediction tasks.
**Tasks & Skills:**

* **MLP:** Predicted football match outcomes and simulated FIFA World Cup results.
* **CNN:** Classified flowers using VGG-style CNN from scratch and fine-tuned ResNet50; applied data augmentation.
* **RNN/LSTM:** Forecasted Bitcoin prices using sequence modeling with different lookback windows.
* Implemented deep learning workflows: preprocessing → model design → training → evaluation → hyperparameter tuning.

**Tools:** Python, PyTorch, TensorFlow, Pandas, NumPy, Matplotlib/Seaborn

---

### **CA5 – Modern NLP & Image Segmentation**

**Focus:** Semi-supervised learning, embedding-based semantic search, and unsupervised image segmentation.
**Tasks & Skills:**

* **Video Game Reviews:** Implemented pseudo-labeling and active learning for score prediction with few labeled samples.
* **Semantic Search:** Built an embedding-based search engine for Persian Q\&A using **bge-m3** and **LanceDB**.
* **Image Segmentation:** Clustered football players in images using K-Means, DBSCAN, and CNN-based features; evaluated with Dice and IoU metrics.

**Tools:** Python, SentenceTransformers, Word2Vec, OpenCV, Scikit-learn, Pretrained CNNs, LanceDB

---

## **Skills & Techniques Demonstrated**

* **Programming & Data Handling:** Python, Pandas, NumPy
* **Statistical Analysis & Modeling:** Monte Carlo, hypothesis testing, regression, confidence intervals
* **Machine Learning:** Supervised & unsupervised learning, feature engineering, model evaluation
* **Deep Learning:** MLP, CNN, RNN/LSTM, pre-trained models, data augmentation
* **Natural Language Processing:** Text embeddings, semantic search, vector databases, semi-supervised learning
* **Data Visualization & Storytelling:** Matplotlib, Seaborn, Tableau, dashboards
* **Data Engineering & Streaming:** Kafka, Spark Streaming, MongoDB, real-time analytics
* **Computer Vision:** Image preprocessing, clustering, semantic segmentation

---

## **Workflow & Best Practices**

* Clear **train/test splits** to avoid data leakage.
* **Data preprocessing pipelines** for reproducibility and consistency.
* Hyperparameter tuning and model comparison for robust performance.
* Visualization of both **results and intermediate steps** to support insights.
* Modular scripts and notebooks to allow step-by-step execution.

---

## **Getting Started**

1. Clone this repository:

```bash
git clone https://github.com/MehdyMokhtari/data-science-mini-projects.git
cd data-science-mini-projects
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Navigate to any CA folder (`CA0`–`CA5`) and run notebooks or scripts.
4. Follow documentation in each folder for dataset setup, preprocessing, and execution instructions.




If you want, I can **also create a visually appealing “Skills & Tools Table”** section for this README that quickly summarizes all your tools and methods in one glance—it looks very professional on GitHub.

Do you want me to do that?
