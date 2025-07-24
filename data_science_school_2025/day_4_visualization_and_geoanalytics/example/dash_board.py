#bokeh serve --show dash_board.py


from bokeh.io import curdoc
from bokeh.layouts import column
from bokeh.models import Slider, ColumnDataSource
from bokeh.plotting import figure
import pandas as pd

# Підготовка даних
x = list(range(1, 11))
y = [i**2 for i in x]
source = ColumnDataSource(data={'x': x, 'y': y})

# Створення графіку
plot = figure(title="Простий дашборд", x_axis_label='x', y_axis_label='y')
plot.line('x', 'y', source=source)

# Повзунок для вибору максимальної межі для значень x
slider = Slider(start=1, end=10, value=10, step=1, title="Максимальне значення x")

# Функція оновлення графіку
def update_plot(attr, old, new):
    max_x = slider.value
    new_y = [i**2 for i in range(1, max_x + 1)]
    source.data = {'x': list(range(1, max_x + 1)), 'y': new_y}

# Підключення повзунка до функції оновлення
slider.on_change('value', update_plot)

# Додавання елементів до макета і запуск додатку
layout = column(slider, plot)
curdoc().add_root(layout)
