import pandas as pd
import glob
import os
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib  # for saving the model

# === CONFIG ===
DATA_DIR = "."  # Folder containing your CSVs
MODEL_OUTPUT = "option_model.pkl"

def load_and_combine_csvs(directory):
    all_files = glob.glob(os.path.join(directory, "*.csv"))
    df_list = []

    for file in all_files:
        try:
            df = pd.read_csv(file)
            if df.dropna(axis=1, how="all").empty:
                print(f"Skipping empty or all-NA file: {file}")
                continue
            df_list.append(df)
        except Exception as e:
            print(f"Skipping {file} due to read error: {e}")

    if not df_list:
        raise ValueError("No valid CSV files found.")
    
    return pd.concat(df_list, ignore_index=True)

def preprocess_data(df):
    # Drop non-feature columns
    df = df.drop(columns=["symbol", "timestamp", "futurePrice"], errors="ignore")

    # Drop rows with missing values
    df = df.dropna()

    df["label"] = df["label"].astype(int)
    # Separate features and target
    X = df.drop(columns=["label"])
    y = df["label"]

    return X, y

def train_model(X, y):
    # Split into train/test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Scale the features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train a Random Forest Classifier
    clf = RandomForestClassifier(random_state=42)
    clf.fit(X_train_scaled, y_train)

    # Evaluate
    y_pred = clf.predict(X_test_scaled)
    print("\n=== Classification Report ===")
    print(classification_report(y_test, y_pred))

    # Save model and scaler
    joblib.dump(clf, MODEL_OUTPUT)
    joblib.dump(scaler, "scaler.pkl")
    print(f"\nModel saved to '{MODEL_OUTPUT}' and scaler to 'scaler.pkl'.")

def main():
    print(f"Loading CSVs from '{DATA_DIR}'...")
    combined_df = load_and_combine_csvs(DATA_DIR)
    print(f"Loaded {len(combined_df)} rows.")

    X, y = preprocess_data(combined_df)
    train_model(X, y)

if __name__ == "__main__":
    main()