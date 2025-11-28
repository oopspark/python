import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import yaml


class Gg:
    def __init__(self, data=None):
        if data is None:
            self.data = None
        elif isinstance(data, pd.DataFrame):
            self.data = data.copy()
        elif isinstance(data, dict):
            self.data = pd.DataFrame(data)
        else:
            raise TypeError("data must be DataFrame or dict")

        self.mapping = {}
        self.geom = None
        self.theme = {}
        self.title = ""

    # ---------------- aes ----------------
    def aes(self, **kwargs):
        self.mapping.update(kwargs)
        return self

    # ---------------- geoms ----------------
    def geom_bar(self):
        self.geom = "bar"
        return self

    def geom_point(self):
        self.geom = "point"
        return self

    def geom_line(self):
        self.geom = "line"
        return self

    # ---------------- theme ----------------
    def theme_from_yaml(self, theme_name, yaml_path):
        with open(yaml_path, "r", encoding="utf-8") as f:
            theme_all = yaml.safe_load(f)
        if theme_name not in theme_all:
            raise ValueError(f"Theme '{theme_name}' not found in YAML")

        self.theme = theme_all[theme_name]
        return self

    def set_title(self, t):
        self.title = t
        return self

    # ---------------- drawing ----------------
    def draw(self):
        if self.data is None:
            raise ValueError("No data provided")

        self._apply_theme()

        x = self.data[self.mapping["x"]]
        y = self.data[self.mapping["y"]]

        # ----- Geom rendering -----
        if self.geom == "bar":
            conf = self.theme["bar"]
            plt.bar(
                x,
                y,
                color=conf["color"],
                edgecolor=conf["edge_color"],
                width=conf["width"],
            )

        elif self.geom == "point":
            conf = self.theme["marker"]
            plt.scatter(
                x,
                y,
                s=conf["size"] ** 2,  # matplotlib의 marker size는 px^2
                color=conf["color"],
                marker=self._marker_symbol(conf["shape"]),
            )

        elif self.geom == "line":
            conf = self.theme["line"]
            plt.plot(
                x,
                y,
                linewidth=conf["width"],
                color=conf["color"],
                linestyle=conf["style"],
                marker=None,
            )

        # ----- text -----
        font_color = self.theme["font"]["color"]
        plt.xlabel(self.mapping["x"], color=font_color)
        plt.ylabel(self.mapping["y"], color=font_color)

        if self.theme["title"]["show"]:
            t = self.theme["title"]
            plt.title(self.title,
                      fontsize=t["size"],
                      fontweight=t["weight"],
                      color=t["color"])

        # ----- legend -----
        if self.theme["legend"]["show"]:
            plt.legend(loc=self.theme["legend"]["location"],
                       fontsize=self.theme["legend"]["font_size"])

        return self

    # ---------------- theme application ----------------
    def _apply_theme(self):
        th = self.theme

        # figure & background
        plt.figure(facecolor=th["background"])
        self.ax = plt.gca()

        # font
        mpl.rcParams["font.family"] = th["font"]["family"]
        mpl.rcParams["font.size"] = th["font"]["size"]

        # grid
        if th["grid"]:
            self.ax.grid(True, color=th["grid_color"])
        else:
            self.ax.grid(False)

        # axis
        if not th["axis"]["show"]:
            self.ax.axis("off")
        else:
            for spine in self.ax.spines.values():
                spine.set_color(th["axis"]["color"])
                spine.set_linewidth(th["axis"]["linewidth"])
            self.ax.tick_params(
                axis="both",
                which="both",
                length=3 if th["axis"]["ticks"] else 0,
                color=th["axis"]["color"],
            )

    # ---------------- util ----------------
    def _marker_symbol(self, shape):
        return {"circle": "o", "square": "s", "triangle": "^"}.get(shape, "o")

    # ---------------- show / save ----------------
    def show(self):
        plt.show()
        return self

    def save(self, path):
        plt.savefig(path, bbox_inches="tight")
        return self
