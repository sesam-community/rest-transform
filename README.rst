====================
sesam-rest-transform
====================

Microservice that calls a REST service (with optional payload) and stores the result in a configurable property.

.. image:: https://travis-ci.org/sesam-community/rest-transform.svg?branch=master
    :target: https://travis-ci.org/sesam-community/rest-transform

* Listens on port 5001 by default
* results are streamed back

Environment Parameters
######################

.. csv-table::
   :header: "CONFIG_NAME","DESCRIPTION","IS_REQUIRED","DEFAULT_VALUE"

   "LOG_LEVEL", "Logging level.", "no", "INFO"
   "PORT", "the port that the service will run on", "no", "5001"
   "PROPERTY", "the property that will contain the transformation result", "no", "response"
   "URL", "the url of the system that will provide transformed data", "yes", "n/a"
   "HEADERS", "headers in json format for the URL", "no", "n/a"
   "AUTHORIZATION", "auth config for the requests to the URL. see below for the valid structure templates", "no", "5000"
   "DO_STREAM", "Flag to receive responses from this service streamed or not. Streaming will be faster but will always return 200", "no", "true"

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
::

    [{
      "_id": "my-rest-transform-system",
      "type": "system:microservice",
      "docker": {
        "environment": {
          "HEADERS": {
            "Accept": "application/json; version=2",
            "Authorization": "token my-travis-token"
          },
          "URL": "https://api.travis-ci.org/settings/env_vars?repository_id={{ repo_id }}",
          "DO_STREAM": false,
          "PROPERTY": "mytransformfield"
        },
        "image": "sesamcommunity/sesam-rest-transform",
        "port": 5001
      }
    },
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
            ["copy", "*"],
            ["add", "::repo_id", "_S.id"]
          ]
        }
      }, {
        "type": "http",
        "system": "my-rest-transform-system",
        "url": "/transform"
      }, {
        "type": "dtl",
        "rules": {
          "default": [
            ["add", "details", "_S.response"],
            ["add", "_id", "_S.name"],
            ["add", "name", "_S.name"]
          ]
        }
      }]
    }]

In this case the entities passed to the transform require a p


Examples:

::

   $ curl -s -XPOST 'http://localhost:5001/transform' -H "Content-type: application/json" -d '[{ "_id": "jane", "name": "Jane Doe" }]' ", " jq -S .
   [
     {
       "_id": "jane",
       "response": "foo-response",
       "name": "Jane Doe"
     }
   ]

::

   $ curl -s -XPOST 'http://localhost:5001/transform' -H "Content-type: application/json" -d @sample.json ", "jq -S .
   [
     {
       "_id": "jane",
       "response": "foo-response",
       "name": "Jane Doe"
     },
     {
       "_id": "john",
       "response": "foo-response",
       "name": "John Smith"
     }
   ]

Note the example uses `curl <https://curl.haxx.se/>`_ to send the request and `jq <https://stedolan.github.io/jq/>`_ prettify the response.
