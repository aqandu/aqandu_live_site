# AQandU
These are instructions for setting up the Python Virtual Environment and frontend of AQandU. We use Python 3 at its latest version (on GCP) which, at the time of writing, is 3.7. These instructions assume that you have python 3.7 and pip installed locally.
  
# Setting up a dev environment

First clone the repo locally and copy the example_config.py to config.py

```
git clone https://github.com/visdesignlab/aqandu.git
cp example_config.py config.py
```

Now, make sure you have pipenv installed using:

```
pip install pipenv
```

Now let's install the python environment and all the dependencies with:

```
pipenv install
```

Next, we need to generate some flask assets with:

```
pipenv run build-assets
```

Then launch the application with:

```
pipenv run serve
```


# Deploying to GCP

To deploy the application, you have to use the command line and the gcloud tools. Once you have the production config (from Jack or another admin) and you've set up gcloud cli with the correct default project, run the following commands:

```
cp config.production.py config.py
gcloud app deploy app.yaml
```

This will start building the containers that serve the website. You can check for a successful deployment from the app engine versions dashboard in GCP. My testing has shown that it takes about 10 minutes to build.

**NOTE**

If you're getting `Error Response: [4] DEADLINE_EXCEEDED` then you need to increase the timeout for the build to 20 minutes using 

```
gcloud config set app/cloud_build_timeout 1200
```