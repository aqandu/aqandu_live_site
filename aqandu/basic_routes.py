from aqandu import app
from flask import render_template


@app.route("/", methods=["GET"])
def index():
    return render_template('main.html')


@app.route("/team", methods=["GET"])
def team():
    return render_template('team.html')


@app.route("/request_sensor", methods=["GET"])
def request_sensor():
    return render_template('request_sensor.html')


@app.route("/airu_sensor", methods=["GET"])
def airu_sensor():
    return render_template('airu_sensor.html')


@app.route("/project", methods=["GET"])
def project():
    return render_template('project.html')


@app.route("/newsroom", methods=["GET"])
def newsroom():
    return render_template('newsroom.html')


@app.route("/mailinglist", methods=["GET"])
def mailinglist():
    return render_template('mailinglist.html')


@app.route("/sensor_FAQ", methods=["GET"])
def sensor_FAQ():
    return render_template('sensor_FAQ.html')


@app.route("/about", methods=["GET"])
def about():
    return render_template('about.html')
