import os
import threading
import gc

from repodata_tools.releases import get_latest_links

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse


# https://stackoverflow.com/questions/12435211/python-threading-timer-repeat-function-every-n-seconds
def setInterval(interval):
    def decorator(function):
        def wrapper(*args, **kwargs):
            stopped = threading.Event()

            def loop():  # executed in another thread
                while not stopped.wait(interval):  # until stopped
                    function(*args, **kwargs)

            t = threading.Thread(target=loop)
            t.daemon = True  # stop if the program exits
            t.start()
            return stopped
        return wrapper
    return decorator


LINKS = get_latest_links()


@setInterval(300)  # every 5 minutes
def _update_links():
    print("************* RELOADING LINKS *************")
    global LINKS
    new_links = get_latest_links()
    LINKS = new_links
    gc.collect()


_stop_update_links = _update_links()

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "this is the index!"}


################################################################################
# labels
################################################################################

@app.get("/conda-forge-sparta/label/{label}")
async def root_label(label):
    return {"message": "this is the index!"}
    # return RedirectResponse(
    #     f"https://regro.github.io/repodata/label/{label}/index.html"
    # )


@app.get("/conda-forge-sparta/label/{label}/channeldata.json")
async def channeldata_label(label):
    fn = f"channeldata_{label}.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/channeldata.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/")
async def subdir_root_label(label, subdir):
    return {"message": "this is the index!"}
    # return RedirectResponse(
    #     f"https://regro.github.io/repodata/label/{label}/{subdir}/index.html"
    # )


@app.get("/conda-forge-sparta/label/{label}/{subdir}/repodata.json")
async def subdir_repodatadata_label(label, subdir):
    fn = f"repodata_{subdir}_{label}.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/repodata.json",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/repodata.json.bz2")
async def subdir_repodatadatabz2_label(label, subdir):
    fn = f"repodata_{subdir}_{label}.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/repodata.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/repodata_from_packages.json")
async def subdir_repodatadata_pkgs_label(label, subdir):
    fn = f"repodata_from_packages_{subdir}_{label}.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/repodata_from_packages.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/repodata_from_packages.json.bz2")
async def subdir_repodatadatabz2_pkgs_label(label, subdir):
    fn = f"repodata_from_packages_{subdir}_{label}.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/repodata_from_packages.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/current_repodata.json")
async def subdir_repodatadata_curr_label(label, subdir):
    fn = f"current_repodata_{subdir}_{label}.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/current_repodata.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/current_repodata.json.bz2")
async def subdir_repodatadatabz2_curr_label(label, subdir):
    fn = f"current_repodata_{subdir}_{label}.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"label/{label}/{subdir}/current_repodata.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/label/{label}/{subdir}/{pkg}")
async def subdir_pkg_label(label, subdir, pkg):
    subdir_pkg = os.path.join(subdir, pkg)
    url = LINKS["packages"].get(subdir_pkg, None)
    if url is None:
        raise HTTPException(
            status_code=404, detail=f"label/{label}/{subdir_pkg} not found!"
        )
    return RedirectResponse(url)


################################################################################
# main
################################################################################


@app.get("/conda-forge-sparta/")
async def root_main():
    return {"message": "this is the index!"}
    # return RedirectResponse("https://regro.github.io/repodata/label/main/index.html")


@app.get("/conda-forge-sparta/channeldata.json")
async def channeldata():
    fn = "channeldata_main.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail="channel_data.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/")
async def subdir_root(subdir):
    return {"message": "this is the index!"}
    # return RedirectResponse(
    #     f"https://regro.github.io/repodata/label/main/{subdir}/index.html"
    # )


@app.get("/conda-forge-sparta/{subdir}/repodata.json")
async def subdir_repodatadata(subdir):
    fn = f"repodata_{subdir}_main.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/repodata.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/repodata.json.bz2")
async def subdir_repodatadatabz2(subdir):
    fn = f"repodata_{subdir}_main.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/repodata.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/repodata_from_packages.json")
async def subdir_repodatadata_pkgs(subdir):
    fn = f"repodata_from_packages_{subdir}_main.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/repodata_from_packages_{subdir}_main.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/repodata_from_packages.json.bz2")
async def subdir_repodatadatabz2_pkgs(subdir):
    fn = f"repodata_from_packages_{subdir}_main.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/repodata_from_packages_{subdir}_main.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/current_repodata.json")
async def subdir_repodatadata_curr(subdir):
    fn = f"current_repodata_{subdir}_main.json"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/current_repodata_{subdir}_main.json not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/current_repodata.json.bz2")
async def subdir_repodatadatabz2_curr(subdir):
    fn = f"current_repodata_{subdir}_main.json.bz2"
    url = LINKS["serverdata"].get(fn, [None])[-1]
    if url is None:
        raise HTTPException(
            status_code=404,
            detail=f"{subdir}/current_repodata_{subdir}_main.json.bz2 not found!",
        )
    return RedirectResponse(url)


@app.get("/conda-forge-sparta/{subdir}/{pkg}")
async def subdir_pkg(subdir, pkg):
    subdir_pkg = os.path.join(subdir, pkg)
    url = LINKS["packages"].get(subdir_pkg, None)
    if url is None:
        raise HTTPException(
            status_code=404, detail=f"{subdir_pkg} not found!"
        )
    return RedirectResponse(url)
