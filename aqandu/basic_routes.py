from aqandu import app
from flask import render_template

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
