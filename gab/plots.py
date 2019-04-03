import pandas
import matplotlib.pyplot as plt
import numpy as np
import seaborn

def drawCDF(a, ax = None, label = None, ccdf = False, minVal = 0, rescaleX = False):
    if ax is None:
        fig, ax = plt.subplots(figsize = (10, 7))
    a_sort = np.sort(a[a >= minVal])
    p = np.arange(len(a_sort)) / (len(a_sort) - 1)
    if rescaleX:
        a_sort = a_sort / np.max(a_sort)
    if ccdf:
        ax.plot(a_sort, 1 - p, label = label)
    else:
        ax.plot(a_sort, p, label = label)
    return ax

def drawSet(df, targets, ccdf = False, minVal = 0, combined_only = False):
    fig_bottom, axes_bottom = plt.subplots(figsize = (16, 5), ncols = 4)
    for t in targets:
        if not combined_only:
            fig, axes = plt.subplots(figsize = (16, 5), ncols = 4)
        for i in range(2):
            for j in range(2):
                if not combined_only:
                    ax = axes[i* 2 + j]
                    drawCDF(df[t], ax, t, ccdf = ccdf, minVal = minVal)
                ax_bottom = axes_bottom[i* 2 + j]
                drawCDF(df[t], ax_bottom, t, ccdf = ccdf, minVal = minVal, rescaleX=True)
                ax_bottom.set_title("{} {} min {} {}-{}".format('All', 'CCDF' if ccdf else 'CDF',minVal,'log' if j else 'lin', 'log' if i else 'lin'))
                if not combined_only:
                    ax.set_title("{} {} min {} {}-{}".format(t, 'CCDF' if ccdf else 'CDF', minVal,'log' if j else 'lin', 'log' if i else 'lin'))

                if i:
                    if not combined_only:
                        ax.set_xscale('log')
                    ax_bottom.set_xscale('log')
                if j:
                    if not combined_only:
                        ax.set_yscale('log')
                    ax_bottom.set_yscale('log')
                if not combined_only:
                    ax.legend()
                    ax.set_ylabel('$P(X < x)$')
                    ax.set_xlabel('$x$')

                ax_bottom.legend()
                ax_bottom.set_ylabel('$P(X < x)$')
                ax_bottom.set_xlabel('$x$')
        if not combined_only:
            fig.tight_layout()
    fig_bottom.tight_layout()
    plt.show()

#From https://github.com/karthik/wesanderson
wes_colours = {
    "BottleRocket1" : ["#A42820", "#5F5647", "#9B110E", "#3F5151", "#4E2A1E", "#550307", "#0C1707"],
    "BottleRocket2" : ["#FAD510", "#CB2314", "#273046", "#354823", "#1E1E1E"],
    "Rushmore1" : ["#E1BD6D", "#EABE94", "#0B775E", "#35274A" ,"#F2300F"],
    "Rushmore" : ["#E1BD6D", "#EABE94", "#0B775E", "#35274A" ,"#F2300F"],
    "Royal1" : ["#899DA4", "#C93312", "#FAEFD1", "#DC863B"],
    "Royal2" : ["#9A8822", "#F5CDB4", "#F8AFA8", "#FDDDA0", "#74A089"],
    "Zissou1" : ["#3B9AB2", "#78B7C5", "#EBCC2A", "#E1AF00", "#F21A00"],
    "Darjeeling1" : ["#FF0000", "#00A08A", "#F2AD00", "#F98400", "#5BBCD6"],
    "Darjeeling2" : ["#ECCBAE", "#046C9A", "#D69C4E", "#ABDDDE", "#000000"],
    "Chevalier1" : ["#446455", "#FDD262", "#D3DDDC", "#C7B19C"],
    "FantasticFox1" : ["#DD8D29", "#E2D200", "#46ACC8", "#E58601", "#B40F20"],
    "Moonrise1" : ["#F3DF6C", "#CEAB07", "#D5D5D3", "#24281A"],
    "Moonrise2" : ["#798E87", "#C27D38", "#CCC591", "#29211F"],
    "Moonrise3" : ["#85D4E3", "#F4B5BD", "#9C964A", "#CDC08C", "#FAD77B"],
    "Cavalcanti1" : ["#D8B70A", "#02401B", "#A2A475", "#81A88D", "#972D15"],
    "GrandBudapest1" : ["#F1BB7B", "#FD6467", "#5B1A18", "#D67236"],
    "GrandBudapest2" : ["#E6A0C4", "#C6CDF7", "#D8A499", "#7294D4"],
    "IsleofDogs1" : ["#9986A5", "#79402E", "#CCBA72", "#0F0D0E", "#D9D0D3", "#8D8680"],
    "IsleofDogs2" : ["#EAD3BF", "#AA9486", "#B6854D", "#39312F", "#1C1718"],
}
