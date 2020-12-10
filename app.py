import os
import gc
import hmac
import hashlib

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

from repodata_tools.links import get_latest_links

LINKS = get_latest_links()

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "this is the index!"}


@app.post("/update-links")
async def update_links(request: Request, status_code=204):
    body = await request.body()
    signature = request.headers.get('X-Hub-Signature', '')
    our_hash = hmac.new(
        os.environ['CF_SPARTA_TOKEN'].encode('utf-8'),
        body,
        hashlib.sha1,
    ).hexdigest()
    their_hash = signature.split("=")[1]

    if not hmac.compare_digest(their_hash, our_hash):
        raise HTTPException(
            status_code=403,
            detail="invalid request",
        )
    else:
        blob = await request.json()
        event = request.headers.get('X-GitHub-Event', None)
        if event == "ping":
            return "pong"
        elif blob["action"] == "released":
            print("**************** UPDATING LINKS ****************", flush=True)
            global LINKS
            new_links = get_latest_links()
            LINKS = new_links
            gc.collect()
            print("**************** DONE UPDATING LINKS ****************", flush=True)


################################################################################
# labels
################################################################################

@app.get("/conda-forge-sparta/label/{label}")
async def root_label(label):
    return {"message": "this is the index!"}


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
