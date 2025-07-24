import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

fig, ax = plt.subplots()
xdata, ydata = [], []
ln, = ax.plot([], [], 'ro')
X = np.random.random()
Y = np.random.random()

def init():
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    xdata.append(0)
    ydata.append(0)
    xdata.append(0.5)
    ydata.append(1)
    xdata.append(1)
    ydata.append(0)
    xdata.append(X)
    ydata.append(Y)
    return ln,

def update(frame):
    # i =np.random.randint(3)
    # X1 = xdata[i]
    # Y1 = ydata[i]
    # X = (X1 - xdata[-1])/ 2 + xdata[-1]
    # Y = (Y1 - ydata[-1]) / 2 + ydata[-1]
    X = np.random.random()
    Y = np.random.random()
    xdata.append(X)
    ydata.append(Y)
    ln.set_data(xdata, ydata)
    return ln,

ani = FuncAnimation(fig, update,
                    init_func=init, blit=True, interval = 1)
plt.show()