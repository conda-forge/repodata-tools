import time
import base64
import os
import io
import sys
from contextlib import redirect_stdout, redirect_stderr

import github
import jwt
import requests
from cryptography.hazmat.backends import default_backend
from nacl import encoding, public


def _encrypt_github_secret(public_key, secret_value):
    """Encrypt a Unicode string using the public key."""
    public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def generate_app_token(app_id, raw_pem):
    """Get an app token.

    Parameters
    ----------
    app_id : str
        The github app ID.
    raw_pem : bytes
        An app private key as bytes.

    Returns
    -------
    gh_token : str
        The github token. May return None if there is an error.
    """
    try:
        if raw_pem[0:1] != b'-':
            raw_pem = base64.b64decode(raw_pem)

        f = io.StringIO()
        with redirect_stdout(f), redirect_stderr(f):
            private_key = default_backend().load_pem_private_key(raw_pem, None)

            ti = int(time.time())
            token = jwt.encode(
                {
                    'iat': ti,
                    'exp': ti + 60*10,
                    'iss': app_id,
                },
                private_key,
                algorithm='RS256',
            )

        if (
            "GITHUB_ACTIONS" in os.environ
            and os.environ["GITHUB_ACTIONS"].lower() == "true"
        ):
            sys.stdout.flush()
            print("masking JWT token for github actions", flush=True)
            print("::add-mask::%s" % token, flush=True)

        with redirect_stdout(f), redirect_stderr(f):
            r = requests.get(
                "https://api.github.com/app/installations",
                headers={
                    'Authorization': 'Bearer %s' % token,
                    'Accept': 'application/vnd.github.machine-man-preview+json',
                },
            )
            r.raise_for_status()

            r = requests.post(
                "https://api.github.com/app/installations/"
                "%s/access_tokens" % r.json()[0]["id"],
                headers={
                    'Authorization': 'Bearer %s' % token,
                    'Accept': 'application/vnd.github.machine-man-preview+json',
                },
            )
            r.raise_for_status()

            gh_token = r.json()["token"]

        if (
            "GITHUB_ACTIONS" in os.environ
            and os.environ["GITHUB_ACTIONS"].lower() == "true"
        ):
            sys.stdout.flush()
            print("masking GITHUB token for github actions", flush=True)
            print("::add-mask::%s" % gh_token, flush=True)

    except Exception:
        gh_token = None

    return gh_token


def get_github_client_with_app_token(app_id_env, private_key_env):
    """Get a github client with an app token.

    Parameters
    ----------
    app_id_env : str
        The name of the environment variable with the app id.
    private_key_env : str
        The name of the environment variable with the private key.

    Returns
    -------
    gh : github.Github
        The github client object. May return None if there is an error.
    """
    try:
        token = generate_app_token(
            os.environ[app_id_env],
            os.environ[private_key_env].encode(),
        )
        if token is not None:
            gh = github.Github(token)
        else:
            gh = None
    except Exception:
        gh = None

    return gh
