import csv
import sys
import math
from sklearn.metrics import mean_squared_error

def compute_rmse1(pred_file, true_file):
    # Read predictions into a dictionary: key = (user_id, business_id), value = predicted stars
    predictions = {}
    with open(pred_file, "r", encoding="utf-8") as pf:
        reader = csv.DictReader(pf)
        for row in reader:
            key = (row["user_id"], row["business_id"])
            predictions[key] = float(row["prediction"])
    
    # Read true ratings into a dictionary: key = (user_id, business_id), value = true stars
    true_ratings = {}
    with open(true_file, "r", encoding="utf-8") as tf:
        reader = csv.DictReader(tf)
        for row in reader:
            key = (row["user_id"], row["business_id"])
            true_ratings[key] = float(row["stars"])
    
    # Collect common keys and build lists of predicted and true values
    y_pred = []
    y_true = []
    missing = []
    for key, pred_val in predictions.items():
        if key in true_ratings:
            y_pred.append(pred_val)
            y_true.append(true_ratings[key])
        else:
            missing.append(key)
    
    if not y_pred:
        print("No common keys found between predictions and true ratings.")
        return
    
    # Compute RMSE using scikit-learn's mean_squared_error function
    mse = mean_squared_error(y_true, y_pred)
    rmse = math.sqrt(mse)
    
    print("RMSE1:", rmse)
    if missing:
        print("Warning: The following keys were missing in the true ratings:", missing)



def compute_rmse2(pred_file, true_file):
    # Read predictions into a dictionary: key = (user_id, business_id), value = predicted stars
    predictions = {}
    with open(pred_file, "r", encoding="utf-8") as pf:
        reader = csv.DictReader(pf)
        for row in reader:
            key = (row["user_id"], row["business_id"])
            predictions[key] = float(row["prediction"])
    
    # Read true ratings into a dictionary: key = (user_id, business_id), value = true stars
    true_ratings = {}
    with open(true_file, "r", encoding="utf-8") as tf:
        reader = csv.DictReader(tf)
        for row in reader:
            key = (row["user_id"], row["business_id"])
            true_ratings[key] = float(row["stars"])
    
    # Compute squared errors for each common key
    squared_errors = []
    missing = []
    for key, pred_val in predictions.items():
        if key in true_ratings:
            true_val = true_ratings[key]
            squared_errors.append((pred_val - true_val) ** 2)
        else:
            missing.append(key)
    
    if not squared_errors:
        print("No common keys found between predictions and true ratings.")
        return

    mse = sum(squared_errors) / len(squared_errors)
    rmse = math.sqrt(mse)

    print("RMSE2:", rmse)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compute_rmse.py <predictions_csv> <true_ratings_csv>")
        sys.exit(-1)
    
    pred_file = sys.argv[1]
    true_file = sys.argv[2]
    compute_rmse1(pred_file, true_file)
    compute_rmse2(pred_file, true_file)

