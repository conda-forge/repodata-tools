import time
import base64
import os
import io
import sys
from contextlib import redirect_stdout, redirect_stderr

import github
import click
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
        if raw_pem[0] != b'-':
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
            and os.environ["GITHUB_ACTIONS"] == "true"
        ):
            sys.stdout.flush()
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
            and os.environ["GITHUB_ACTIONS"] == "true"
        ):
            sys.stdout.flush()
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

    if gh is not None:
        print("renewed github client", flush=True)

    return gh


@click.command()
@click.argument("app_id", type=str)
@click.option(
    "--pem",
    type=str,
    default=None,
    help="Path to private key in PEM format."
)
@click.option(
    "--env-var",
    type=str,
    default=None,
    help="Name of environment variable with base64 encoded PEM."
)
def main_gen(app_id, pem, env_var):
    """Generate a GitHub token using app APP_ID from a private key.

    NOTE: The token is printed to stdout, so make sure not to use this command
    in public CI infrastructure.
    """

    if pem is None and env_var is None:
        raise RuntimeError("One of --pem or --env-var must be given!")

    try:
        f = io.StringIO()
        with redirect_stdout(f), redirect_stderr(f):
            if pem is not None:
                with open(pem, "r") as fp:
                    raw_pem = fp.read().encode()
            else:
                raw_pem = base64.b64decode(os.environ[env_var])

            token = generate_app_token(app_id, raw_pem)

        print(token)

    except Exception:
        pass


@click.command()
@click.argument("app_id", type=str)
@click.argument("target_repo", type=str)
@click.argument("secret_name", type=str)
@click.option(
    "--pem",
    type=str,
    default=None,
    help="Path to private key in PEM format."
)
@click.option(
    "--env-var",
    type=str,
    default=None,
    help="Name of environment variable with base64 encoded PEM."
)
def main_push(app_id, target_repo, secret_name, pem, env_var):
    """Generate a GitHub token using app APP_ID from a private key.
    Then push this token to the TARGET_REPO's secrets w/ SECRET_NAME."""

    if pem is None and env_var is None:
        raise RuntimeError("One of --pem or --env-var must be given!")

    try:
        f = io.StringIO()
        with redirect_stdout(f), redirect_stderr(f):
            if pem is not None:
                with open(pem, "r") as fp:
                    raw_pem = fp.read().encode()
            else:
                raw_pem = base64.b64decode(os.environ[env_var])

            token = generate_app_token(app_id, raw_pem)

            rkey = requests.get(
                "https://api.github.com/repos/"
                "%s/actions/secrets/public-key" % target_repo,
                headers={
                    "Authorization": "Bearer %s" % os.environ["GITHUB_TOKEN"],
                    "Accept": "application/vnd.github.v3+json",
                }
            )
            rkey.raise_for_status()

            encoded_token = _encrypt_github_secret(rkey.json()["key"], token)

            requests.put(
                "https://api.github.com/repos/%s/actions/secrets/%s" % (
                    target_repo,
                    secret_name,
                ),
                headers={
                    "Authorization": "Bearer %s" % os.environ["GITHUB_TOKEN"],
                    "Accept": "application/vnd.github.v3+json",
                },
                json={
                    "encrypted_value": encoded_token,
                    "key_id": rkey.json()["key_id"],
                }
            ).raise_for_status()

    except Exception:
        pass
