import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
import matplotlib.patches as mpatches
import matplotlib as mpl
import numpy as np


def cmap(
    dfg: gpd.GeoDataFrame,
    var: str,
    dfg_bounds=None,
    bins=[-100, -25, 0, 25, 100, 10000000000],
    cmap_col: str = "RdYlBu_r",
    title_leg: str = "Pers/km²",
    png: str = None,
    xlim=None,
    ylim=None,
    context=False,
):

    bounds = []
    for index, upper_bound in enumerate(bins):
        if index == 0:
            bound = f"< {upper_bound:.0f}"

        elif index == (len(bins) - 1):
            lower_bound = bins[index - 1]
            bound = f"> {lower_bound:.0f}"

        else:
            lower_bound = bins[index - 1]
            bound = f"{lower_bound:.0f} - {upper_bound:.0f}"

        bounds.append(bound)

    if context == True:
        alpha = 0.4
    else:
        alpha = 1

    ax = dfg.plot(
        var,
        cmap=cmap_col,
        scheme="UserDefined",
        alpha=alpha,
        legend=True,
        legend_kwds=dict(
            loc="lower left",
            labels=bounds,
            fontsize=15,
            markerscale=1,
            frameon=True,
            title=title_leg,
            title_fontsize=15,
        ),
        classification_kwds={"bins": bins},
        figsize=(10, 10),
    )
    if context == True:
        ctx.add_basemap(
            ax,
            zoom=13,
            crs=dfg.crs.to_string(),
            source=ctx.providers.OpenStreetMap.Mapnik,
        )

    if dfg_bounds is not None:
        dfg_bounds.boundary.plot(ax=ax, color="black", linewidth=2)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_axis_off()

    if xlim is not None:
        ax.set_xlim(xlim)
    if ylim is not None:
        ax.set_ylim(ylim)
    if png is not None:
        plt.savefig(png, format="png")


def imshow_raster(
    raster: np.array,
    cmap_col: str = "RdYlBu_r",
    png: str = None
):


    fig = plt.figure(figsize=(10,10))

    ax = fig.add_subplot(111)
    im = plt.imshow(raster, cmap = cmap_col, origin = "lower")
    plt.colorbar(im)

    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_axis_off()

    if png is not None:
        plt.savefig(png, format="png")

        
def cmap_raster(
    raster: np.array,
    bins=[1000, 3000, 5000, 10000],
    cmap_col: str = "RdYlBu_r",
    title_leg: str = "Bias (m)",
    png: str = None,
    legend = True
):

    bounds = []
    for index, upper_bound in enumerate(bins):
        if index == 0:
            bound = f"< {upper_bound:.0f}"

        elif index == (len(bins) - 1):
            lower_bound = bins[index - 1]
            bound = f"> {lower_bound:.0f}"

        else:
            lower_bound = bins[index - 1]
            bound = f"{lower_bound:.0f} - {upper_bound:.0f}"

        bounds.append(bound)
        
    cmap = mpl.cm.get_cmap(cmap_col) 
    norm = mpl.colors.BoundaryNorm(bins, cmap.N, extend='both')
    
    fig = plt.figure(figsize=(10,10))
    ax = fig.add_subplot(111)
        

    im = plt.imshow(raster, origin = "lower", cmap = cmap, norm = norm)
    colors = [ im.cmap(im.norm(value)) for value in bins]
    patches = [ mpatches.Patch(color=colors[i], label=bounds[i] ) for i in range(len(bounds)) ]
    if legend == True:
        plt.legend(handles=patches,
               loc="lower left",
            fontsize=14,
            markerscale=1,
            frameon=True,
            title=title_leg,
            title_fontsize=14)
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_axis_off()

    if png is not None:
        plt.savefig(png, format="png")
