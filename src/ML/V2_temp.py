import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import SGDRegressor, SGDClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, log_loss
)


# ============================================================
# 🧠 ML 파이프라인 클래스 (batch/epoch + loss 기록/시각화)
# ============================================================
class MLRunner:
    def __init__(self, X, y, task_type="regression", model_name="sgd",
                 scaler_type="standard", batch_size=None, epochs=1000, learning_rate=0.01):
        self.X = X
        self.y = y
        self.task_type = task_type
        self.model_name = model_name
        self.scaler_type = scaler_type
        self.batch_size = batch_size
        self.epochs = epochs
        self.learning_rate = learning_rate
        self.train_loss_per_epoch = []
        self.valid_loss_per_epoch = []

    # ==========================================
    # 데이터 전처리
    # ==========================================
    def preprocess(self, test_size=0.2, random_state=42):
        self.X_train, self.X_valid, self.y_train, self.y_valid = train_test_split(
            self.X, self.y, test_size=test_size, random_state=random_state
        )

        if self.scaler_type == "standard":
            scaler = StandardScaler()
        elif self.scaler_type == "minmax":
            scaler = MinMaxScaler()
        else:
            scaler = None

        if scaler:
            self.X_train = scaler.fit_transform(self.X_train)
            self.X_valid = scaler.transform(self.X_valid)

        print(f"✅ Preprocess done ({self.scaler_type or 'no scaling'})")

    # ==========================================
    # 모델 초기화
    # ==========================================
    def init_model(self):
        if self.task_type == "regression":
            if self.model_name == "sgd":
                self.model = SGDRegressor(
                    random_state=42,
                    max_iter=1,
                    learning_rate='constant',
                    eta0=self.learning_rate,
                    warm_start=True
                )
            elif self.model_name in ["tree", "rf"]:
                self.model = RandomForestRegressor(random_state=42)
            else:
                raise ValueError("지원하지 않는 회귀 모델 이름입니다.")
        else:
            if self.model_name == "sgd":
                self.model = SGDClassifier(
                    random_state=42,
                    max_iter=1,
                    learning_rate='constant',
                    eta0=self.learning_rate,
                    warm_start=True
                )
            elif self.model_name == "tree":
                self.model = DecisionTreeClassifier(random_state=42)
            elif self.model_name == "rf":
                self.model = RandomForestClassifier(random_state=42)
            else:
                raise ValueError("지원하지 않는 분류 모델 이름입니다.")

        print(f"🧩 Model initialized: {self.model_name.upper()} ({self.task_type})")

    # ==========================================
    # SGD 배치 학습 + train/valid 손실 기록
    # ==========================================
    def _fit_sgd_in_batches(self):
        n_samples = len(self.X_train)
        batch_size = self.batch_size or n_samples  # full-batch 기본
        self.train_loss_per_epoch = []
        self.valid_loss_per_epoch = []

        for epoch in range(self.epochs):
            indices = np.random.permutation(n_samples)
            X_shuffled = self.X_train[indices]
            y_shuffled = self.y_train.iloc[indices] if isinstance(self.y_train, pd.Series) else self.y_train[indices]

            for start in range(0, n_samples, batch_size):
                end = start + batch_size
                X_batch = X_shuffled[start:end]
                y_batch = y_shuffled[start:end]

                if self.task_type == "regression":
                    self.model.partial_fit(X_batch, y_batch)
                else:
                    self.model.partial_fit(X_batch, y_batch, classes=np.unique(self.y_train))

            # epoch 끝날 때 손실 기록
            y_pred_train = self.model.predict(self.X_train)
            y_pred_valid = self.model.predict(self.X_valid)

            if self.task_type == "regression":
                train_loss = mean_squared_error(self.y_train, y_pred_train)
                valid_loss = mean_squared_error(self.y_valid, y_pred_valid)
            else:
                train_loss = log_loss(self.y_train, self.model.predict_proba(self.X_train))
                valid_loss = log_loss(self.y_valid, self.model.predict_proba(self.X_valid))

            self.train_loss_per_epoch.append(train_loss)
            self.valid_loss_per_epoch.append(valid_loss)

            if (epoch + 1) % max(1, self.epochs // 10) == 0:
                print(f"Epoch {epoch + 1}/{self.epochs} - Train Loss: {train_loss:.4f}, Valid Loss: {valid_loss:.4f}")

    # ==========================================
    # 학습 및 평가
    # ==========================================
    def train_and_evaluate(self):
        if self.model_name == "sgd":
            self._fit_sgd_in_batches()
        else:
            self.model.fit(self.X_train, self.y_train)

        y_pred_train = self.model.predict(self.X_train)
        y_pred_valid = self.model.predict(self.X_valid)

        if self.task_type == "regression":
            metrics = {
                "Train MSE": mean_squared_error(self.y_train, y_pred_train),
                "Valid MSE": mean_squared_error(self.y_valid, y_pred_valid),
                "Train RMSE": np.sqrt(mean_squared_error(self.y_train, y_pred_train)),
                "Valid RMSE": np.sqrt(mean_squared_error(self.y_valid, y_pred_valid)),
                "Train R2": r2_score(self.y_train, y_pred_train),
                "Valid R2": r2_score(self.y_valid, y_pred_valid)
            }
            # 계수/절편
            self.coef_ = self.model.coef_
            self.intercept_ = self.model.intercept_
        else:
            metrics = {
                "Accuracy": accuracy_score(self.y_valid, y_pred_valid),
                "Precision": precision_score(self.y_valid, y_pred_valid, average='weighted'),
                "Recall": recall_score(self.y_valid, y_pred_valid, average='weighted'),
                "F1": f1_score(self.y_valid, y_pred_valid, average='weighted')
            }

        print("\n📊 Evaluation Results ----------------")
        for k, v in metrics.items():
            print(f"{k:12s}: {v:.4f}")

        if self.task_type == "regression":
            print("\n📌 Coefficients:", self.coef_)
            print("📌 Intercept:", self.intercept_)

        self.y_pred_valid = y_pred_valid
        self.metrics = metrics

    # ==========================================
    # 결과 시각화
    # ==========================================
    def visualize(self):
        plt.figure(figsize=(8, 6))
        if self.task_type == "regression":
            sns.scatterplot(x=self.y_valid, y=self.y_pred_valid, alpha=0.7)
            plt.plot(
                [self.y_valid.min(), self.y_valid.max()],
                [self.y_valid.min(), self.y_valid.max()],
                'r--', lw=2
            )
            plt.title(f"{self.model_name.upper()} Regression")
            plt.xlabel("Actual")
            plt.ylabel("Predicted")
        else:
            cm = confusion_matrix(self.y_valid, self.y_pred_valid)
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            plt.title(f"{self.model_name.upper()} Classification Matrix")
            plt.xlabel("Predicted")
            plt.ylabel("Actual")
        plt.tight_layout()
        plt.show()

    # ==========================================
    # 손실 추이 시각화 (train/valid)
    # ==========================================
    def visualize_loss(self):
        if self.model_name != "sgd":
            print("Loss per epoch 시각화는 SGD 모델에만 적용됩니다.")
            return
        plt.figure(figsize=(8,5))
        plt.plot(range(1, self.epochs+1), self.train_loss_per_epoch, marker='o', alpha=0.7, label='Train Loss')
        plt.plot(range(1, self.epochs+1), self.valid_loss_per_epoch, marker='x', alpha=0.7, label='Valid Loss')
        plt.xlabel("Epoch")
        plt.ylabel("Loss")
        plt.title(f"{self.model_name.upper()} Loss per Epoch")
        plt.legend()
        plt.grid(True)
        plt.show()


def main():
    from sklearn.datasets import load_diabetes

    data = load_diabetes()
    X = pd.DataFrame(data.data, columns=data.feature_names)
    y = pd.Series(data.target)

    reg = MLRunner(
        X, y,
        task_type="regression",
        model_name="sgd",
        scaler_type="standard",
        batch_size=32,
        epochs=50,
        learning_rate=0.01
    )

    reg.preprocess()
    reg.init_model()
    reg.train_and_evaluate()
    reg.visualize()
    reg.visualize_loss()


# ============================================================
# 🚀 예시 실행
# ============================================================
if __name__ == "__main__":
    main()