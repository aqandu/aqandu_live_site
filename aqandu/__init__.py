# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from flask import Flask, render_template

import assets


def create_app(config, debug=False, testing=False, config_overrides=None):
    app = Flask(__name__)
    app.config.from_object(config)

    app.debug = debug
    app.testing = testing

    assets.init(app)

    if config_overrides:
        app.config.update(config_overrides)

    # Configure logging
    if not app.testing:
        logging.basicConfig(level=logging.INFO)

    # Add a default root route.
    @app.route("/")
    def index():
        return render_template('main.html')

    @app.route("/team")
    def team():

        return render_template('team.html')

    @app.route("/request_sensor")
    def request_sensor():

        return render_template('request_sensor.html')

    @app.route("/airu_sensor")
    def airu_sensor():

        return render_template('airu_sensor.html')

    @app.route("/project")
    def project():

        return render_template('project.html')

    @app.route("/newsroom")
    def newsroom():

        return render_template('newsroom.html')

    @app.route("/mailinglist")
    def mailinglist():

        return render_template('mailinglist.html')

    @app.route("/sensor_FAQ")
    def sensor_FAQ():

        return render_template('sensor_FAQ.html')

    @app.route("/about")
    def about():

        return render_template('about.html')

    # Add an error handler. This is useful for debugging the live application,
    # however, you should disable the output of the exception for production
    # applications.
    @app.errorhandler(500)
    def server_error(e):
        return """
        An internal error occurred: <pre>{}</pre>
        See logs for full stacktrace.
        """.format(e), 500

    return app
