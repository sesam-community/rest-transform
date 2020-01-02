from flask import Flask, request, Response
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import json
import os
import requests
import datetime
from jinja2 import Template
from sesamutils import sesam_logger

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 5001))

logger = sesam_logger("rest-transform-service")

prop = os.environ.get("PROPERTY", "response")
method = os.environ.get("METHOD", "GET").upper()
url_template = Template(os.environ["URL"])
headers = json.loads(os.environ.get("HEADERS", "{}"))
authorization = os.environ.get("AUTHORIZATION")
do_stream =  os.environ.get("DO_STREAM", "true").lower() == "true"

session_factory = None

class BasicUrlSystem():
    def __init__(self, config):
        self._config = config

    def make_session(self):
        session = requests.Session()
        session.auth = tuple(self._config.get("basic")) if self._config.get("basic") else None
        session.headers = self._config.get("headers")
        return session


class Oauth2System():
    def __init__(self, config):
        """init Oauth2Client with a json config"""
        self._config = config
        self._get_token()

    def _get_token(self):
        # If no token has been created yet or if the previous token has expired, fetch a new access token
        # before returning the session to the callee
        if not hasattr(self, "_token") or self._token["expires_at"] <= datetime.datetime.now().timestamp():
            oauth2_client = BackendApplicationClient(client_id=self._config["oauth2"]["client_id"])
            session = OAuth2Session(client=oauth2_client)
            logger.debug("Updating token...")
            self._token = session.fetch_token(**self._config["oauth2"])

        logger.debug("expires_at[{}] - now[{}]={} seconds remaining".format(self._token["expires_at"],datetime.datetime.now().timestamp(), self._token["expires_at"] - datetime.datetime.now().timestamp()))
        return self._token

    def make_session(self):
        token = self._get_token()
        client = BackendApplicationClient(client_id=self._config["oauth2"]["client_id"])
        session = OAuth2Session(client=client, token=token)
        session.headers = self._config["headers"]
        return session

if authorization:
    logger.debug(str(type(authorization)))
    authorization = json.loads(authorization)
    logger.debug(str(type(authorization)))
    if authorization.get("type", "") == "oauth2":
        session_factory = Oauth2System({"oauth2": authorization.get("oauth2"), "headers": headers})
    else:
        session_factory = BasicUrlSystem({"basic": authorization.get("basic"), "headers": headers})
else:
        session_factory = BasicUrlSystem({"headers": headers})

@app.route("/transform", methods=["POST"])
def receiver():

    def generate(entities):
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","
            url = url_template.render(entity=entity)
            with session_factory.make_session() as s:
                logger.debug(f"url={url}, entity={entity}")
                if method == "GET":
                    resp = s.get(url, headers=headers)
                else:
                     resp = s.request(method, url, data=entity.get("payload"),
                                                headers=headers)
                logger.debug(f'transform of entity with _id={entity.get("_id","?")} received {resp.status_code}')
                entity[prop] = resp.json()
            yield json.dumps(entity)
        yield "]"

    # get entities from request
    entities = request.get_json()
    response_data_generator = generate(entities)
    response_data = []
    if do_stream:
        response_data = response_data_generator
    else:
        for entity in response_data_generator:
            response_data.append(entity)
    return Response(
        response=response_data, mimetype="application/json")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=PORT)
