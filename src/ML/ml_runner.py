# ml_runner.py
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor, SGDClassifier
from sklearn.metrics import mean_squared_error, log_loss

class MLRunner:
    def __init__(self, X, y, task_type="regression", model_name="sgd",
                 scaler_type="standard", batch_size=32, epochs=50, learning_rate=0.01):
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

    def preprocess(self, test_size=0.2):
        self.X_train, self.X_valid, self.y_train, self.y_valid = train_test_split(
            self.X, self.y, test_size=test_size, random_state=42
        )
        if self.scaler_type == "standard":
            scaler = StandardScaler()
            self.X_train = scaler.fit_transform(self.X_train)
            self.X_valid = scaler.transform(self.X_valid)

    def init_model(self):
        if self.task_type == "regression":
            self.model = SGDRegressor(max_iter=1, learning_rate='constant',
                                      eta0=self.learning_rate, warm_start=True)
        else:
            self.model = SGDClassifier(max_iter=1, learning_rate='constant',
                                       eta0=self.learning_rate, warm_start=True,
                                       loss='log_loss')

    def fit_epoch(self):
        n_samples = len(self.X_train)
        indices = np.random.permutation(n_samples)
        X_shuffled = self.X_train[indices]
        y_shuffled = self.y_train.iloc[indices] if hasattr(self.y_train, "iloc") else self.y_train[indices]
        batch_size = self.batch_size

        for start in range(0, n_samples, batch_size):
            end = start + batch_size
            X_batch = X_shuffled[start:end]
            y_batch = y_shuffled[start:end]
            if self.task_type == "regression":
                self.model.partial_fit(X_batch, y_batch)
            else:
                self.model.partial_fit(X_batch, y_batch, classes=np.unique(self.y_train))

        # 손실 계산
        if self.task_type == "regression":
            y_pred_train = self.model.predict(self.X_train)
            y_pred_valid = self.model.predict(self.X_valid)
            train_loss = mean_squared_error(self.y_train, y_pred_train)
            valid_loss = mean_squared_error(self.y_valid, y_pred_valid)
        else:
            y_pred_train = self.model.predict_proba(self.X_train)
            y_pred_valid = self.model.predict_proba(self.X_valid)
            train_loss = log_loss(self.y_train, y_pred_train)
            valid_loss = log_loss(self.y_valid, y_pred_valid)

        self.train_loss_per_epoch.append(train_loss)
        self.valid_loss_per_epoch.append(valid_loss)
        return train_loss, valid_loss
