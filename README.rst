====================
rest-transform
====================

.. image:: https://github.com/sesam-community/rest-transform/actions/workflows/sesam-community-ci-cd.yml/badge.svg
   :target: https://github.com/sesam-community/rest-transform/actions/workflows/sesam-community-ci-cd.yml

Microservice that calls a URL (with optional payload) and able to store the result in a configurable property.

* can be used as sink or transform
* entity level customization
* transform results are streamed back by default and streaming can be turned off.
* Listens on port 5001 by default
* can be configured to tolerate errors - to avoid pump execution failure

Endpoints
######################
.. csv-table::
  :header: "ENDPOINT","METHOD(S)", "DESCRIPTION"

  "/transform", "POST", "endpoint to be used as transform service, i.e. in http_transform"
  "/sink", "POST", "endpoint to be used as sink."

Query Parameters
######################

.. csv-table::
   :header: "NAME","DESCRIPTION"

   "service_config_property", "the property in the entity that serves for the entity specific execution parameters. Overrules the corresponding env var."
   "path", "the path for the endpoint on the target url. The value is appended to the URL to reveal the final URL. See examples section for usage alternatives."

service_config_property, if specified must point to a dict in the incoming entities. The dict fields must be a subset of environment variables.
refer to "Environment Parameters" section for their explanations and eligable values.

Environment Parameters
######################

.. csv-table::
  :header: "CONFIG_NAME","DESCRIPTION","IS_REQUIRED","DEFAULT_VALUE"

  "AUTHORIZATION", "auth config for the requests to the URL. see below for the valid structure templates", "no", "5000"
  "LOG_LEVEL", "Logging level.", "no", "'INFO'"
  "PORT", "the port that the service will run on", "no", "5001"
  "DO_STREAM", "Flag to receive responses from this service streamed or not. Streaming will be faster but will always return 200", "no", "true"
  "DO_VERIFY_SSL", "Flag to enable/disable ssl verification", "no", "false"
  "METHOD *", "http method for the call to URL", "no", "'GET'"
  "PROPERTY *", "the property that will contain the transformation result", "no", "'response'"
  "PAYLOAD_PROPERTY_FOR_TRANSFORM_REQUEST *", "the property that will contain the payload to be sent to URL", "no", "'payload'"
  "URL *", "the url of the system that will provide transformed data", "yes", "n/a"
  "HEADERS *", "headers in json format for the URL", "no", "n/a"
  "TOLERABLE_STATUS_CODES *", "regex pattern to be searched on response code from the URL. If matched, error won't be raised but returned( in the field as specified in ""PROPERTY"" variable) so that pipe won't fail. The response will be in this format: {""transform_succeeded"": true|false, ""message"": ""<error message>"", ""status_code"": <http code for error>}. Applicable to `/transform` endpoint only", "no", "none"
  "SERVICE_CONFIG_PROPERTY", "the key that points to the service_config_property in the input entities", "no", "'service_config'"

*: can be overruled by utilizing entity level customization via service_config_property envvar/query param.

AUTHORIZATION, if specified, can have following structures

No auth:
::

    "AUTHORIZATION": None

Basic Auth
::

    "AUTHORIZATION": {
      "type": "basic",
      "basic": ["my username", "my password"]
    }

Oauth2:
::

    "AUTHORIZATION": {
      "type": "oauth2",
      "oauth2": {
        "client_id": "my oauth2 client",
        "client_secret": "my oauth2 secret",
        "token_url": "my oauth2's token url"
      }
    }






Example config:
########

System:
::

    {
      "_id": "my-rest-transform-system",
      "type": "system:microservice",
      "docker": {
        "environment": {
          "HEADERS": {
            "Accept": "application/json; version=2",
            "Authorization": "token my-travis-token"
          },
          "URL": "https://my_domain/my_script_root",
          "DO_STREAM": false,
          "PROPERTY": "mytransformresponse",
          "TOLERABLE_STATUS_CODES": "404|400"
        },
        "image": "sesamcommunity/sesam-rest-transform",
        "port": 5001
      }
    }


Pipe:
::
  {
    "_id": "my-transform-pipe",
    "type": "pipe",
    "source": {
      "type": "dataset",
      "dataset": "my-source"
    },
    "transform": [{
      "type": "dtl",
      "rules": {
        "default": [
          ["copy", "_id"],
          ["add", "id", "_S._id"]
        ]
      }
    }, {
      "type": "http",
      "system": "my-rest-transform-system",
      "url": "/transform?path=/mypath/to/myresource?myid={{ id }}"
    }, {
      "type": "dtl",
      "rules": {
        "default": [
          ["copy", "_id"],
          ["if",
            ["eq", "_S.mytransformresponse.transform_succeeded", false],
            ["comment", "optionally, do the tolerated error handling here, or alternatively in a succeeding pipe"],
            ["add", "details", "_S.mytransformresponse"]
          ]
        ]
      }
    }]
  }
  
  


Following combinations are all equivalent for the examplified case:  


.. csv-table::
   :header: "URL (ENVVAR)","PATH(query param)","ENTITY (payload- partially shown)","Comments"
   
   https://my_domain/my_script_root, /mypath/to/myresource?myid={{ id }},  ..."id":10... , Most generic usage of the microservice
   https://my_domain/my_script_root/mypath/to/, myresource?myid={{ id }}, ..."id":10 ..., A less generic usage
   https://my_domain/my_script_root/mypath/to/myresource?myid={{ id }}, , ..."id":10..., Least generic usage due to rigid URL value
   https://my_domain/my_script_root/{{ _my_full_path}}, , ..."_my_full_path":"mypath/to/myresource?myid=10"..., "Most generic usage where replacement value is not in the entity"
