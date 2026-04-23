"""
train.py
────────
Trains an XGBoost classifier on historical transaction data
to predict the probability of fraud.
Saves the trained model to the models/ directory.
"""

import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib
import os
import sys

# For simplicity in this project, we'll train on the synthetic data we generated
# In reality, this would be trained on historical, labeled data.
DATA_PATH = os.path.join(os.path.dirname(__file__), '../../data/transactions.csv')
MODEL_DIR = os.path.join(os.path.dirname(__file__), '../../models')

def run_training():
    print("🚀 Starting model training pipeline...")
    
    if not os.path.exists(DATA_PATH):
        print(f"❌ Error: Data file not found at {DATA_PATH}")
        print("Please run generate_transactions.py first.")
        sys.exit(1)
        
    print("📊 Loading data...")
    df = pd.read_csv(DATA_PATH)
    
    # Feature Engineering (simplified for demo)
    # We drop PII and non-numeric columns for the basic model
    features = ['amount']
    # Add dummy variables for transaction_type
    df_encoded = pd.get_dummies(df, columns=['transaction_type'], drop_first=True)
    
    # Get all numeric columns
    numeric_cols = df_encoded.select_dtypes(include=['number']).columns.tolist()
    feature_cols = [c for c in numeric_cols if c != 'is_fraud']
    
    X = df_encoded[feature_cols]
    y = df_encoded['is_fraud']
    
    print(f"Features used: {feature_cols}")
    
    print("✂️ Splitting data into train/test sets...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("🧠 Training XGBoost model...")
    # scale_pos_weight is used because fraud is a rare event (imbalanced classes)
    model = XGBClassifier(scale_pos_weight=20, random_state=42, use_label_encoder=False, eval_metric='logloss')
    model.fit(X_train, y_train)
    
    print("✅ Model trained. Evaluating...")
    y_pred = model.predict(X_test)
    
    print("\n" + "="*40)
    print("Classification Report:")
    print("="*40)
    print(classification_report(y_test, y_pred))
    
    # Save the model and the feature names so the API knows what to expect
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, 'fraud_model.pkl')
    features_path = os.path.join(MODEL_DIR, 'model_features.pkl')
    
    joblib.dump(model, model_path)
    joblib.dump(feature_cols, features_path)
    
    print(f"💾 Model saved to: {model_path}")

if __name__ == "__main__":
    run_training()
