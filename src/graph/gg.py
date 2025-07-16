import pandas as pd
import numpy as np
import seaborn as sns
import yaml
import matplotlib.pyplot as plt
import matplotlib as mpl


class DataMixin():
    def __init__(self):
        super().__init__()
        self.data = self._data_process(self.data)

    def _data_process(self, data):
        if data is None:
            self.data = None
        elif isinstance(data, dict):
            self.data = pd.DataFrame(data)
        elif 'pandas' in str(type(data)):
            self.data = data.copy()
        elif isinstance(data, list):
            self.data = self._process_list_data(data)
        else:
            raise TypeError("data must be dict, pandas DataFrame, or list")
        return self.data

    def _process_list_data(self, data):
        coord_val_pairs, ndim = self._parse_array_to_coords_and_values(data)
        coords = [coord for coord, _ in coord_val_pairs]
        values = [val for _, val in coord_val_pairs]

        coord_cols = self._generate_dim_names(ndim)
        df = pd.DataFrame(coords, columns=coord_cols)
        df['value'] = values
        return df

    def _parse_array_to_coords_and_values(self, data):
        arr = np.array(data)

        if arr.dtype == object:
            raise ValueError("입력된 리스트는 정규 다차원 배열이 아닙니다. 내부 리스트 길이가 모두 같아야 합니다.")

        coord_val_pairs = []

        for idx in np.ndindex(arr.shape):
            val = arr[idx]
            coord_val_pairs.append(((idx), val))

        return coord_val_pairs, len(arr.shape)

    def _generate_dim_names(self, n):
        base_names = ['x', 'y', 'z']
        if n <= 3:
            return base_names[:n]
        else:
            extra_dims = [chr(i) for i in range(ord('a'), ord('a') + n - 3)]
            return base_names + extra_dims

class AesMixin():
    def __init__(self):
        super().__init__()
        self.mapping = {}

    def aes(self, **kwargs):
        self.mapping.update(kwargs)
        return self
    
class GeomMixin():
    def __init__(self):
        super().__init__()
        self.geom = None
        self.geom_engine_map = {
            'bar': 'matplotlib',
            'point': 'matplotlib',
            'stacked': 'matplotlib',
            'histogram': 'matplotlib',
            'line': 'matplotlib',
            'scatter': 'matplotlib',
            'box': 'matplotlib',
            'pie': 'matplotlib',
            'heat': 'matplotlib',
            'area': 'matplotlib',
            'stream': 'matplotlib',
            'confuse': 'matplotlib',
            'duration': 'matplotlib',
            'violin': 'seaborn',
            'pair': 'seaborn',
            'density': 'seaborn',
            'bubble': 'matplotlib',
            'decision': 'console',
            'decomposition': 'console',
            'event': 'console',
            'spider': 'plotly',
            'treemap': 'plotly',
            'chord': 'plotly',
            'sankey': 'plotly',
            'network': 'networkx',
            'icicle': 'plotly',
            'parallel': 'plotly',
            'surface': 'plotly',
        }
        self.goem_engine = None

    def geom_bar(self):
        self.geom = 'bar'
        self.geom_engine = self.geom_engine_map.get(self.geom, 'unknown')
        if self.data is None:
            example_data = {'category': ['A', 'B', 'C'], 'value': np.random.randint(5, 20, 3)}
            self.data = pd.DataFrame(example_data)
            self.mapping = {'x': 'category', 'y': 'value'}
        return self
    def geom_point(self):
        self.geom = 'point'
        self.geom_engine = self.geom_engine_map.get(self.geom, 'unknown')
        if self.data is None:
            example_data = {'x': list(range(10)), 'y': np.random.randint(0, 100, 10)}
            self.data = pd.DataFrame(example_data)
            self.mapping = {'x': 'x', 'y': 'y'}
        return self
    def geom_line(self):
        self.geom = 'line'
        self.geom_engine = self.geom_engine_map.get(self.geom, 'unknown')
        if self.data is None:
            example_data = {'time': list(range(20)), 'value': np.cumsum(np.random.randn(20))}
            self.data = pd.DataFrame(example_data)
            self.mapping = {'x': 'time', 'y': 'value'}
        return self
    # ... (다른 geom 메서드도 이 파일에 추가) ...

class ThemeMixin():
    def __init__(self):
        super().__init__()
        self.theme = {}
        
    def set_theme_from_yaml(self, theme_name, yaml_path=r'C:\Users\parkj\Documents\workspace\my_projects\0_basic\gg_theme.yml'):
        with open(yaml_path, 'r', encoding='utf-8') as f:
            theme_data = yaml.safe_load(f)
        if theme_name not in theme_data:
            raise ValueError(f"Theme '{theme_name}' not found in {yaml_path}")
        self.theme = theme_data[theme_name]
        return self

    def theme_minimal(self): return self.set_theme_from_yaml("minimal")
    def theme_dark(self): return self.set_theme_from_yaml("dark")
    def theme_modern(self): return self.set_theme_from_yaml("modern")
    def theme_presentation(self): return self.set_theme_from_yaml("presentation")
    def theme_basic(self): return self.set_theme_from_yaml("basic")

class DrawMixin(GeomMixin, ThemeMixin):
    def __init__(self):
        super().__init__()
        self.ax = None
        self.last_fig = None

    def draw(self):
        if self.geom_engine in ['matplotlib', 'seaborn']:
            self._apply_theme_matplotlib()
            self._apply_text_matplotlib()
            self._draw_matplotlib_based()
        elif self.geom_engine == 'plotly':
            self._draw_plotly_based()
        elif self.geom_engine == 'networkx':
            self._draw_networkx_based()
        elif self.geom_engine == 'console':
            self._draw_console_based()
        else:
            raise ValueError(f"Unknown geom engine: {self.geom_engine}")
        return self

    def _parse_rgba_string(self, rgba_str):
        if isinstance(rgba_str, str) and rgba_str.startswith('rgba'):
            rgba_str = rgba_str[5:-1]  # 'rgba('와 ')' 제거
            r, g, b, a = map(float, rgba_str.split(','))
            return (r/255, g/255, b/255, a)
        return rgba_str  # 이미 튜플이거나 문자열이면 그대로


    def _apply_theme_matplotlib(self):
        theme = self.theme
        bg = theme.get('background', 'white')
        bg = self._parse_rgba_string(bg) if isinstance(bg, str) else bg
        plt.figure(facecolor=bg)
        font = theme.get('font', {})
        mpl.rcParams['font.family'] = font.get('family', 'Arial')
        mpl.rcParams['font.size'] = font.get('size', 12)
        self.ax = plt.gca()
        if theme.get('grid', False):
            self.ax.grid(True, color=theme.get('grid_color', 'lightgray'))
        else:
            self.ax.grid(False)
        axis = theme.get('axis', {})
        if not axis.get('show', True):
            self.ax.axis('off')
        else:
            self.ax.axis('on')
        for spine in self.ax.spines.values():
            spine.set_color(axis.get('color', 'black'))
            spine.set_linewidth(axis.get('linewidth', 1))
        self.ax.tick_params(
            axis='both',
            which='both',
            length=3 if axis.get('ticks', True) else 0,
            color=axis.get('color', 'black')
        )
        title = theme.get('title', {})
        self._title_conf = {
            "show": title.get('show', True),
            "size": title.get('size', 14),
            "weight": title.get('weight', 'bold'),
            "color": title.get('color', 'black')
        }
        legend = theme.get('legend', {})
        self._legend_conf = {
            "show": legend.get('show', True),
            "loc": legend.get('location', 'best'),
            "fontsize": legend.get('font_size', 10)
        }
        bar = theme.get('bar', {})
        self._bar_conf = {
            "color": bar.get('color', 'skyblue'),
            "edgecolor": bar.get('edge_color', 'black'),
            "width": bar.get('width', 0.8)
        }
        line = theme.get('line', {})
        self._line_conf = {
            "color": line.get('color', 'blue'),
            "width": line.get('width', 2),
            "style": line.get('style', 'solid')
        }
        scatter = theme.get('scatter', {})
        self._scatter_conf = {
            "alpha": scatter.get('alpha', 0.8),
            "colormap": scatter.get('colormap', 'viridis')
        }
        pie = theme.get('pie', {})
        self._pie_conf = {
            "autopct": pie.get('autopct', '%1.1f%%'),
            "startangle": pie.get('startangle', 90)
        }
        violin = theme.get('violin', {})
        self._violin_conf = {
            "inner": violin.get('inner', 'box')
        }
        heatmap = theme.get('heatmap', {})
        self._heatmap_conf = {
            "cmap": heatmap.get('cmap', 'coolwarm'),
            "annot": heatmap.get('annot', False)
        }

    def _apply_text_matplotlib(self):
        font = self.theme.get('font', {})
        color = font.get('color', 'black')
        plt.xlabel(self.mapping.get('x', ''), color=color)
        plt.ylabel(self.mapping.get('y', ''), color=color)
        if hasattr(self, '_title_conf') and self._title_conf.get('show', True):
            plt.title(
                getattr(self, 'title', ''),
                fontsize=self._title_conf['size'],
                fontweight=self._title_conf['weight'],
                color=self._title_conf['color']
            )
        if hasattr(self, '_legend_conf') and self._legend_conf.get('show', True):
            handles, labels = plt.gca().get_legend_handles_labels()
            if labels:
                plt.legend(
                    loc=self._legend_conf['loc'],
                    fontsize=self._legend_conf['fontsize']
                )

    def _draw_matplotlib_based(self):
        geom = self.geom
        df = self.data
        mapping = self.mapping
        if geom == 'bar':
            x = df[mapping['x']]
            y = df[mapping['y']]
            conf = self._bar_conf
            plt.bar(
                x,
                y,
                color=conf['color'],
                edgecolor=conf['edgecolor'],
                width=conf['width']
            )
        elif geom == 'point':
            x = df[mapping['x']]
            y = df[mapping['y']]
            plt.scatter(x, y, color='tomato')
            
        elif geom == 'stacked':
            x = df[mapping['x']]
            y1 = df[mapping['y1']]
            y2 = df[mapping['y2']]
            plt.bar(x, y1, color='skyblue')
            plt.bar(x, y2, bottom=y1, color='salmon')
            
        elif geom == 'histogram':
            values = df[mapping['x']]
            plt.hist(values, bins=10, color='lightgreen', edgecolor='black')
            
        elif geom == 'line':
            x = df[mapping['x']]
            y = df[mapping['y']]
            plt.plot(x, y, marker='o', color='blue')
            
        elif geom == 'scatter':
            x = df[mapping['x']]
            y = df[mapping['y']]
            plt.scatter(x, y, color='purple')
            
        elif geom == 'box':
            data_cols = [mapping[k] for k in mapping if k.startswith('y')]
            data_to_plot = [df[col] for col in data_cols]
            plt.boxplot(data_to_plot, labels=data_cols)
            
        elif geom == 'pie':
            labels = df[mapping['x']]
            sizes = df[mapping['y']]
            plt.pie(sizes, labels=labels, autopct='%1.1f%%')
            
        elif geom == 'heat':
            data = df.pivot(index='y', columns='x', values='value')
            sns.heatmap(data, annot=True, fmt=".2f", cmap="coolwarm")
            
        elif geom == 'area':
            x = df[mapping['x']]
            y = df[mapping['y']]
            plt.fill_between(x, y, color='skyblue', alpha=0.4)
            plt.plot(x, y, color='Slateblue', alpha=0.6)
            
        elif geom == 'violin':
            data_cols = [mapping[k] for k in mapping if k.startswith('y')]
            data_to_plot = [df[col] for col in data_cols]
            sns.violinplot(data=data_to_plot)
            
        elif geom == 'pair':
            sns.pairplot(df)
            
        elif geom == 'density':
            sns.kdeplot(df[mapping['x']], shade=True)
            
        elif geom == 'stream':
            for col in df.columns:
                plt.plot(df.index, df[col], label=col)
            plt.legend()
            
        elif geom == 'confuse':
            sns.heatmap(df, annot=True, fmt="d", cmap="Blues")
            
        elif geom == 'decomposition':
            plt.plot(df.index, df['trend'], label='Trend')
            plt.plot(df.index, df['seasonal'], label='Seasonal')
            plt.plot(df.index, df['residual'], label='Residual')
            plt.legend()
            
        elif geom == 'decision':
            print("Decision conditions and results:")
            for idx, row in df.iterrows():
                print(f"{row[mapping['condition']]} -> {row[mapping['result']]}")
        else:
            raise NotImplementedError(f"Draw not implemented for geom {geom}")

    def _draw_plotly_based(self):
        data = self.data
        if self.geom == 'spider':
            import plotly.express as px
            fig = px.line_polar(data, r=self.mapping['y'], theta=self.mapping['x'], line_close=True)
            
        elif self.geom == 'treemap':
            import plotly.express as px
            fig = px.treemap(data, path=[self.mapping['x']], values=self.mapping['y'])
            
        elif self.geom == 'sankey':
            import plotly.graph_objects as go
            data = data
            fig = go.Figure(go.Sankey(
                node = dict(label = data['nodes'].tolist()),
                link = dict(
                    source = data['source'].tolist(),
                    target = data['target'].tolist(),
                    value = data['value'].tolist()
                )
            ))
            
        elif self.geom == 'icicle':
            import plotly.express as px
            fig = px.icicle(data, path=[self.mapping['x']], values=self.mapping['y'])
            
        elif self.geom == 'parallel':
            import plotly.express as px
            fig = px.parallel_coordinates(data, color=data[self.mapping.get('color', '')])
            
        elif self.geom == 'surface':
            import plotly.graph_objects as go
            z = data.pivot(index='y', columns='x', values='value').values
            fig = go.Figure(data=[go.Surface(z=z)])
            
        else:
            raise NotImplementedError(f"Plotly drawing not implemented for self.geom {self.geom}")
        self.last_fig = fig
        return


    def _draw_networkx_based(self):
        import networkx as nx
        import matplotlib.pyplot as plt
        geom = self.geom
        data = self.data
        if geom == 'network':
            G = nx.Graph()
            nodes = data['nodes'].tolist()
            edges = data['edges'].tolist()
            G.add_nodes_from(nodes)
            G.add_edges_from(edges)
            nx.draw(G, with_labels=True)
            
        else:
            raise NotImplementedError(f"NetworkX drawing not implemented for geom {geom}")

    def _draw_console_based(self):
        geom = self.geom
        data = self.data
        if geom == 'decision':
            pass
        elif geom == 'event':
            print("Event Timeline:")
            for idx, row in data.iterrows():
                print(f"{row[self.mapping.get('x','time')]} : {row[self.mapping.get('y','event')]}")
        else:
            raise NotImplementedError(f"Console drawing not implemented for geom {geom}")

class ShowSaveMixin(DrawMixin):
    def __init__(self):
        super().__init__()
        
    def show(self):
        geom_engine = self.geom_engine
        if geom_engine in ['matplotlib', 'seaborn', 'networkx']:
            plt.show()
        elif geom_engine == 'plotly':
            if self.last_fig is not None:
                self.last_fig.show()

    def save(self, path):
        geom_engine = self.geom_engine
        if geom_engine in ['matplotlib', 'seaborn', 'networkx']:
            plt.savefig(path, bbox_inches='tight')
        elif geom_engine == 'plotly':
            if self.last_fig is not None:
                self.last_fig.write_image(path)

class Gg(DataMixin, AesMixin, ShowSaveMixin):
    def __init__(self, data: pd.DataFrame = None ):
        self.data = data
        super().__init__()

if __name__ == "__main__":

    Gg().geom_bar().draw().show()