# -*- coding: utf-8 -*-
#
# assets.py
#
# Copyright 2016 Socos LLC
#

from os import path
from flask_assets import Bundle, Environment
from flask import Flask


def init(app=None):
    app = app or Flask(__name__)
    with app.app_context():
        env = Environment(app)
        env.directory = 'aqandu/static'
        env.load_path = [path.join(path.dirname(__file__), 'aqandu/static')]
        env.auto_build = False  # App Engine doesn't support automatic rebuilding.
        env.versions = 'hash'
        env.manifest = 'file'

        all_css = Bundle(
            'css/airu.css',
            'css/visualization.css',
            'css/ie10-viewport-bug-workaround.css',
            filters='cssmin', output='css/all_css.%(version)s.css')
        env.register('css', all_css)

        all_js = Bundle(
            'js/db_data.js',
            'js/map_reconfigure.js',
            filters='jsmin', output='js/all_js.%(version)s.js')
        env.register('js', all_js)

        bundles = [all_css, all_js]
        return bundles


if __name__ == '__main__':
    bundles = init()
    for bundle in bundles:
        bundle.build()
