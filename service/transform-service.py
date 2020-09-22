from flask import Flask, request, Response, abort
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import json
import os
import copy
import requests
import datetime
from jinja2 import Template
from sesamutils import sesam_logger
from sesamutils.flask import serve
import re

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 5001))

logger = sesam_logger("rest-transform-service")

prop = os.environ.get("PROPERTY", "response")
payload_property = os.environ.get("PAYLOAD_PROPERTY_FOR_TRANSFORM_REQUEST", "payload")
method = os.environ.get("METHOD", "GET").upper()
url = os.environ["URL"]
headers = json.loads(os.environ.get("HEADERS", "{}"))
authorization = os.environ.get("AUTHORIZATION")
do_stream = os.environ.get("DO_STREAM", "true").lower() == "true"
do_verify_ssl = os.environ.get("DO_VERIFY_SSL", "false").lower() == "true"
tolerable_status_codes = os.environ.get("TOLERABLE_STATUS_CODES")

print(f"starting with {url}, do_stream={do_stream}, prop={prop}, tolerable_status_codes='{tolerable_status_codes}'")

session_factory = None

class BasicUrlSystem():
    def __init__(self, config):
        self._config = config

    def make_session(self):
        session = requests.Session()
        session.auth = tuple(self._config.get("basic")) if self._config.get("basic") else None
        session.headers = self._config["headers"]
        session.verify = do_verify_ssl
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
        session.verify = do_verify_ssl
        return session

if authorization:
    authorization = json.loads(authorization)
    if authorization.get("type", "") == "oauth2":
        session_factory = Oauth2System({"oauth2": authorization.get("oauth2"), "headers": headers})
    else:
        session_factory = BasicUrlSystem({"basic": authorization.get("basic"), "headers": headers})
else:
        session_factory = BasicUrlSystem({"headers": headers})

@app.route("/transform", methods=["POST"], endpoint='transform')
@app.route("/sink", methods=["POST"], endpoint='sink')
def receiver():

    service_config_property = request.args.get("service_config_property", "service_config")
    path = request.args.get("path", "")

    def generate(entities, endpoint):
        yield "["
        with session_factory.make_session() as s:
            for index, entity in enumerate(entities):
                if index > 0:
                    yield ","
                url_per_entity, method_per_entity, headers_per_entity, prop_per_entity, tolerable_status_codes_per_entity = url, method, headers, prop, tolerable_status_codes
                if entity.get(service_config_property):
                    _transform_config = entity.get(service_config_property)
                    url_per_entity = _transform_config.get("URL", url) + path
                    method_per_entity = _transform_config.get("METHOD", method_per_entity)
                    headers_per_entity = copy.deepcopy(_transform_config.get("HEADERS"))
                    prop_per_entity = _transform_config.get("PROPERTY", prop_per_entity)
                    tolerable_status_codes_per_entity = _transform_config.get("TOLERABLE_STATUS_CODES", tolerable_status_codes_per_entity)
                url_template_per_entity = Template(url_per_entity)
                rendered_url = url_template_per_entity.render(entity=entity)

                '''construct the response dict even if errors occur so that it can be handled in dtl.
                   tolerate errors as per configuration
                '''
                transform_result = {}
                try:
                    resp = s.request(method_per_entity, rendered_url, json=entity.get(payload_property),headers=headers_per_entity)
                except Exception as er:
                    transform_result = {"status_code": 500, "return_value": {"transform_succeeded": False, "message": str(er), "status_code": 500}}
                else:
                    if resp.ok:
                        transform_result = {"status_code": resp.status_code, "return_value": resp.json()}
                    else:
                        transform_result = {"status_code": resp.status_code, "return_value": {"transform_succeeded": resp.ok, "message": resp.text, "status_code": resp.status_code}}

                logger.debug(f'transform of entity with _id={entity.get("_id","?")}, prop_per_entity={prop_per_entity} received {transform_result} from {rendered_url}')
                if endpoint == 'transform':
                    if not ((transform_result.get("status_code") >= 200 and transform_result.get("status_code") < 400)
                        or (tolerable_status_codes_per_entity
                        and re.search(tolerable_status_codes_per_entity, str(transform_result.get("status_code"))))):
                        abort(transform_result.get("status_code"), transform_result.get("return_value"))
                    entity[prop_per_entity] = transform_result["return_value"]
                elif endpoint == 'sink':
                    resp.raise_for_status()
                yield json.dumps(entity)
        yield "]"

    # get entities from request
    entities = request.get_json()
    response_data_generator = generate(entities, request.endpoint)
    response_data = []
    if do_stream and request.endpoint != 'sink':
        response_data = response_data_generator
    else:
        for entity in response_data_generator:
            response_data.append(entity)
    return Response(response=response_data, mimetype="application/json")


if __name__ == "__main__":
    serve(app, port=PORT)
