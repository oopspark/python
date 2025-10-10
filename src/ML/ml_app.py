# ml_app.py
import matplotlib.pyplot as plt
from io import BytesIO
from PIL import Image
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static, ListView, ListItem
from textual.containers import Vertical
from textual.reactive import reactive

from ml_runner import MLRunner
import pandas as pd
from sklearn.datasets import load_diabetes

class MLTextualApp(App):
    train_loss = reactive(0.0)
    valid_loss = reactive(0.0)
    epoch = reactive(0)

    def __init__(self, X, y):
        super().__init__()
        self.X = X
        self.y = y
        self.mlrunner = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            yield Static("Select Model:")
            self.model_list = ListView(
                ListItem(Static("SGD")),
                ListItem(Static("DecisionTree")),
                ListItem(Static("RandomForest")),
            )
            yield self.model_list

            yield Static("Select Batch Size:")
            self.batch_list = ListView(
                ListItem(Static("16")),
                ListItem(Static("32")),
                ListItem(Static("64")),
            )
            yield self.batch_list

            yield Static("Select Epochs:")
            self.epoch_list = ListView(
                ListItem(Static("10")),
                ListItem(Static("20")),
                ListItem(Static("50")),
            )
            yield self.epoch_list

            self.status = Static("Press Enter to Start Training")
            yield self.status

            self.plot_widget = Static()
            yield self.plot_widget
        yield Footer()

    async def on_key(self, event):
        if event.key == "enter" and self.mlrunner is None:
            # 선택값 가져오기
            model_name = self.model_list.get_selected().renderable.plain.lower()
            batch_size = int(self.batch_list.get_selected().renderable.plain)
            epochs = int(self.epoch_list.get_selected().renderable.plain)

            # MLRunner 초기화
            self.mlrunner = MLRunner(self.X, self.y, task_type="regression",
                                     model_name=model_name, batch_size=batch_size, epochs=epochs)
            self.mlrunner.preprocess()
            self.mlrunner.init_model()
            self.status.update(f"Training {model_name} | Batch {batch_size} | Epochs {epochs}")

            # 학습 루프
            for epoch in range(1, epochs + 1):
                train_loss, valid_loss = self.mlrunner.fit_epoch()
                self.epoch = epoch
                self.train_loss = train_loss
                self.valid_loss = valid_loss
                self.status.update(f"Epoch {epoch}/{epochs} | Train Loss: {train_loss:.4f} | Valid Loss: {valid_loss:.4f}")

                # Plot update
                fig, ax = plt.subplots()
                ax.plot(range(1, len(self.mlrunner.train_loss_per_epoch)+1), self.mlrunner.train_loss_per_epoch, label="Train")
                ax.plot(range(1, len(self.mlrunner.valid_loss_per_epoch)+1), self.mlrunner.valid_loss_per_epoch, label="Valid")
                ax.set_xlabel("Epoch")
                ax.set_ylabel("Loss")
                ax.legend()
                buf = BytesIO()
                plt.savefig(buf, format='png')
                plt.close(fig)
                buf.seek(0)
                img = Image.open(buf)
                self.plot_widget.update(img)

                await self.sleep(0.1)  # UI 갱신

# ==========================
# 실행
# ==========================
if __name__ == "__main__":
    data = load_diabetes()
    X = pd.DataFrame(data.data, columns=data.feature_names)
    y = pd.Series(data.target)

    app = MLTextualApp(X, y)
    app.run()
