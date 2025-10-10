import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import SGDClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    log_loss, accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
)
from sklearn.utils import shuffle

# ============================================================
# 🧠 ML 파이프라인 클래스 (Train / Valid / Test + log_loss)
# ============================================================
class MLRunner:
    def __init__(self, X, y, model_name="sgd",
                 scaler_type="standard", batch_size=None, epochs=100, learning_rate=0.01):
        self.X = X
        self.y = y
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
    def preprocess(self, test_size=0.1, valid_size=0.2, random_state=42):
        # (train+valid) / test 먼저 분리
        X_temp, self.X_test, y_temp, self.y_test = train_test_split(
            self.X, self.y, test_size=test_size, random_state=random_state, stratify=self.y
        )

        # train / valid 분리
        valid_ratio = valid_size / (1 - test_size)
        self.X_train, self.X_valid, self.y_train, self.y_valid = train_test_split(
            X_temp, y_temp, test_size=valid_ratio, random_state=random_state, stratify=y_temp
        )

        # Scaling
        if self.scaler_type == "standard":
            scaler = StandardScaler()
        elif self.scaler_type == "minmax":
            scaler = MinMaxScaler()
        else:
            scaler = None

        if scaler:
            self.X_train = scaler.fit_transform(self.X_train)
            self.X_valid = scaler.transform(self.X_valid)
            self.X_test = scaler.transform(self.X_test)

        print(f"✅ Preprocess done ({self.scaler_type or 'no scaling'})")

    # ==========================================
    # 모델 초기화
    # ==========================================
    def init_model(self):
        if self.model_name == "sgd":
            self.model = SGDClassifier(
                random_state=42,
                max_iter=1,
                learning_rate='constant',
                eta0=self.learning_rate,
                loss='log_loss',
                warm_start=True
            )
        elif self.model_name == "tree":
            self.model = DecisionTreeClassifier(random_state=42)
        elif self.model_name == "rf":
            self.model = RandomForestClassifier(random_state=42)
        else:
            raise ValueError("지원하지 않는 모델 이름입니다.")

        print(f"🧩 Model initialized: {self.model_name.upper()} (Classification)")

    # ==========================================
    # SGD 배치 학습 + 손실 기록
    # ==========================================
    def _fit_sgd_in_batches(self):
        n_samples = len(self.X_train)
        batch_size = self.batch_size or n_samples
        self.train_loss_per_epoch, self.valid_loss_per_epoch = [], []

        for epoch in range(self.epochs):
            X_shuf, y_shuf = shuffle(self.X_train, self.y_train, random_state=epoch)

            for start in range(0, n_samples, batch_size):
                end = start + batch_size
                X_batch, y_batch = X_shuf[start:end], y_shuf[start:end]
                self.model.partial_fit(X_batch, y_batch, classes=np.unique(self.y_train))

            # epoch 종료 시 손실 기록
            y_pred_train = self.model.predict_proba(self.X_train)
            y_pred_valid = self.model.predict_proba(self.X_valid)

            train_loss = log_loss(self.y_train, y_pred_train)
            valid_loss = log_loss(self.y_valid, y_pred_valid)

            self.train_loss_per_epoch.append(train_loss)
            self.valid_loss_per_epoch.append(valid_loss)

            if (epoch + 1) % max(1, self.epochs // 10) == 0:
                print(f"Epoch {epoch+1}/{self.epochs} - Train Loss: {train_loss:.4f}, Valid Loss: {valid_loss:.4f}")

    # ==========================================
    # 학습 및 평가
    # ==========================================
    def train_and_evaluate(self):
        if self.model_name == "sgd":
            self._fit_sgd_in_batches()
        else:
            self.model.fit(self.X_train, self.y_train)

        # 예측
        y_pred_train = self.model.predict(self.X_train)
        y_pred_valid = self.model.predict(self.X_valid)
        y_pred_test  = self.model.predict(self.X_test)

        y_proba_train = self.model.predict_proba(self.X_train)
        y_proba_valid = self.model.predict_proba(self.X_valid)
        y_proba_test  = self.model.predict_proba(self.X_test)

        # 손실
        train_loss = log_loss(self.y_train, y_proba_train)
        valid_loss = log_loss(self.y_valid, y_proba_valid)
        test_loss  = log_loss(self.y_test, y_proba_test)

        # 메트릭 계산
        def metrics(y_true, y_pred):
            return {
                "Accuracy": accuracy_score(y_true, y_pred),
                "Precision": precision_score(y_true, y_pred, average="weighted"),
                "Recall": recall_score(y_true, y_pred, average="weighted"),
                "F1": f1_score(y_true, y_pred, average="weighted"),
            }

        m_train = metrics(self.y_train, y_pred_train)
        m_valid = metrics(self.y_valid, y_pred_valid)
        m_test  = metrics(self.y_test,  y_pred_test)

        # ===========================
        # 결과 출력 포맷
        # ===========================
        print("\n📢 최종 학습 결과")
        print("  -----------------------------")
        print(f"  📉 Metric : log_loss (cross entropy)")
        print(f"  🔸 Final Train Loss : {train_loss:.4f}")
        print(f"  🔸 Final Valid Loss : {valid_loss:.4f}")
        print(f"  🏅 Final  Test Loss : {test_loss:.4f}\n")

        def print_block(name, metrics):
            print(f"  ✳️ {name} Set")
            print("  -----------------------")
            for k, v in metrics.items():
                print(f"  🔹 {k:<10}: {v:.4f}")
            print()

        print_block("Train", m_train)
        print_block("Valid", m_valid)
        print_block("Test", m_test)

        self.metrics = {"train": m_train, "valid": m_valid, "test": m_test}

    # ==========================================
    # 손실 시각화
    # ==========================================
    def visualize_loss(self):
        if self.model_name != "sgd":
            print("Loss per epoch 시각화는 SGD 모델에만 적용됩니다.")
            return
        plt.figure(figsize=(8, 5))
        plt.plot(self.train_loss_per_epoch, label="Train Loss", marker='o')
        plt.plot(self.valid_loss_per_epoch, label="Valid Loss", marker='x')
        plt.xlabel("Epoch")
        plt.ylabel("Log Loss")
        plt.title("Loss per Epoch")
        plt.legend()
        plt.grid(True)
        plt.show()


# ============================================================
# 🚀 예시 실행: OrganAMNIST 분류
# ============================================================
if __name__ == "__main__":
    from medmnist import OrganAMNIST
    from sklearn.utils import Bunch

    def load_organamnist(limit=5000):
        ds = OrganAMNIST(split='train', download=True)
        X = ds.imgs
        if X.ndim == 4 and X.shape[-1] == 1:
            X = X.squeeze(-1)
        y = ds.labels.squeeze()

        if limit:
            X, y = X[:limit], y[:limit]

        X_flat = X.reshape(len(X), -1)
        return Bunch(data=X_flat, target=y)

    data = load_organamnist(limit=1000)
    X, y = pd.DataFrame(data.data), pd.Series(data.target)

    clf = MLRunner(
        X, y,
        model_name="sgd",
        scaler_type="standard",
        batch_size=64,
        epochs=100,
        learning_rate=0.01
    )

    clf.preprocess()
    clf.init_model()
    clf.train_and_evaluate()
    clf.visualize_loss()
