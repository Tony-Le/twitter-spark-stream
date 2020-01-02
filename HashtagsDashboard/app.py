"""
	This Spark app performs POST requests to updates charts made on this dashboard.

	Made for: EECS 4415 - Big Data Systems (York University EECS dept.)

    Modified by: Tony Le
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat
"""
from flask import Flask,jsonify,request
from flask import render_template
import ast
app = Flask(__name__)
labels = []
values = []
@app.route("/")
def get_chart_page():
	global labels,values
	labels = []
	values = []
	return render_template('chart.html', values=values, labels=labels)
@app.route("/b")
def get_chart_page_b():
	global labels,values
	labels = []
	values = []
	return render_template('chartB.html', values=values, labels=labels)
@app.route('/refreshData')
def refresh_graph_data():
	global labels, values
	print("labels now: " + str(labels))
	print("data now: " + str(values))
	return jsonify(sLabel=labels, sData=values)
@app.route('/updateData', methods=['POST'])
def update_data():
	global labels, values
	if not request.form or 'data' not in request.form:
		return "error", 400
	print("going to print values")
	labels = ast.literal_eval(request.form['label'])
	values = ast.literal_eval(request.form['data'])
	print("labels received: " + str(labels))
	print("data received: " + str(values))
	return "success", 201
@app.route('/b/refreshData')
def refresh_graph_data_b():
	global labels, values
	print("labels now: " + str(labels))
	print("data now: " + str(values))
	return jsonify(sLabel=labels, sData=values)
@app.route('/b/updateData', methods=['POST'])
def update_data_b():
	global labels, values
	if not request.form or 'data' not in request.form:
		return "error", 400
	print("going to print values")
	labels = ast.literal_eval(request.form['label'])
	values = ast.literal_eval(request.form['data'])
	print("labels received: " + str(labels))
	print("data received: " + str(values))
	return "success", 201
if __name__ == "__main__":
	app.debug = True
	app.run(host='localhost', port=5001)
