import json
import requests
import urllib3
import pymysql
from urllib.parse import urlencode
from flask import Flask, request, jsonify


def DialogDBaccess(data):
	conn = pymysql.connect(host='143.248.135.146', port=3142, user='flagship', passwd='kbagent', db='dialogDB',
						   charset='utf8')
	curs = conn.cursor()
	result = None
	if data['mode'] == 'LOGIN':
		sql = "CREATE TABLE IF NOT EXISTS " + data['user_id'] + " LIKE dialog"
		curs.execute(sql)
	elif data['mode'] == 'QUERY':
		sql = data['query']
		curs.execute(sql)
		result = curs.fetchall()
	elif data['mode'] == 'REGISTER':
		sql = "INSERT INTO " + data['user_id'] + " (utterance, date_time, speaker) VALUES(%s, now(), %s)"
		curs.execute(sql, (data['utterance'], data['speaker']))
		result = curs.lastrowid

	conn.commit()
	curs.close()
	conn.close()

	return result


def UserDBaccess(userDB_json):
	userID = userDB_json['userID']
	command = userDB_json['command']
	targetURL = "http://kbox.kaist.ac.kr:6121/flagship"
	requestJson = {
		'user_id': userID,
		'command': command,
	}
	headers = {'Content-Type': 'application/json; charset=utf-8'}

	if command == 'QUERY':
		requestJson['query'] = userDB_json['query']
	elif command == 'REGISTER':
		requestJson['triple'] = userDB_json['triple']

	print(requestJson)
	response = requests.post(targetURL, headers=headers, data=json.dumps(requestJson))
	print("[responseCode] " + str(response.status_code))
	if command == 'REGISTER':
		result = None
	else:
		result = response.json()

	return result


def MasterDBaccess(query):
	server = 'http://kbox.kaist.ac.kr:5820/myDB/'

	values = urlencode({'query': query})
	http = urllib3.PoolManager()
	#print(query)

	headers = {
		'Content-Type': 'application/x-www-form-urlencoded, application/sparql-query, text/turtle',
		'Accept': 'text/turtle, application/rdf+xml, application/n-triples, application/trig, application/n-quads, '
				  'text/n3, application/trix, application/ld+json, '  # application/sparql-results+xml, '
				  'application/sparql-results+json, application/x-binary-rdf-results-table, text/boolean, text/csv, '
				  'text/tsv, text/tab-separated-values '
	}
	url = server + 'query?' + values
	r = http.request('GET', url, headers=headers)
	result = json.loads(r.data.decode('UTF-8'))
	'''print(request)
	if 'SELECT' in query:
		result_list = request['results']['bindings']
	elif 'ASK' in query:
		result_list = request['boolean']
	'''
	return result


app = Flask(__name__)

@app.route('/', methods = ['POST'])
def main():
	input_json = request.get_json()
	result_json = {}
	if 'user_id' in input_json.keys():
		result_json['user_id'] = input_json['user_id']
	else:
		print("empty user_id")
		result_json['result'] = "empty user_id"
		return jsonify(result_json)

	if 'db_type' in input_json.keys():
		result_json['db_type'] = input_json['db_type']
	else:
		print("empty db_type")
		result_json['result'] = "empty db_type"
		return jsonify(result_json)

	if 'mode' in input_json.keys():
		result_json['mode'] = input_json['mode']
	else:
		print("empty mode")
		result_json['result'] = "empty mode"
		return jsonify(result_json)

	if result_json['mode'] == 'LOGIN':
		if result_json['db_type'] == 'userKB':
			userDB_json = {'user_id': result_json['user_id'], 'command': result_json['mode']}
			userDB_result = UserDBaccess(userDB_json)
			result_json['result'] = "userKB created"
		elif result_json['db_type'] == 'dialogDB':
			dialogDB_json = {'user_id': result_json['user_id'], 'mode': result_json['mode']}
			dialogDB_result = DialogDBaccess(dialogDB_json)
			result_json['result'] = "dialogDB created"
		else:
			print("db_type error")
			result_json['result'] = "db_type error"
			return jsonify(result_json)
	elif result_json['mode'] == 'QUERY':
		if result_json['db_type'] == 'userKB':
			result_json['contents'] = input_json['contents']
			userDB_json = {'user_id': result_json['user_id'], 'command': result_json['mode'], 'query': result_json['contents']}
			result_json['result'] = UserDBaccess(userDB_json)
		elif result_json['db_type'] == 'masterKB':
			result_json['contents'] = input_json['contents']
			result_json['result'] = MasterDBaccess(result_json['contents'])
		elif result_json['db_type'] == 'dialogDB':
			result_json['contents'] = input_json['contents']
			dialogDB_json = {'user_id': result_json['user_id'], 'mode': result_json['mode'], 'query': result_json['contents']}
			result_json['result'] = DialogDBaccess(dialogDB_json)
		else:
			print("db_type error")
			result_json['result'] = "db_type error"
			return jsonify(result_json)
	elif result_json['mode'] == 'REGISTER':
		if result_json['db_type'] == 'userKB':
			userDB_json = {'user_id': result_json['user_id'], 'command': result_json['mode'], 'triple': input_json['triples']}
			userDB_result = UserDBaccess(userDB_json)
			result_json['result'] = "triples registered"
		elif result_json['db_type'] == 'dialogDB':
			result_json['contents'] = input_json['contents']
			dialogDB_json = {'user_id': result_json['user_id'], 'mode': result_json['mode'], 'utterance': result_json['contents']['utterance'], 'speaker': result_json['contents']['speaker']}
			dialogDB_result = DialogDBaccess(dialogDB_json)
			if 'triples' in input_json.keys():
				new_triples = []
				for ele_triple in input_json['triples']:
					s, p, o = ele_triple
					new_triples.append(ele_triple)
					new_triples.append([ s+p+o , 'http://kbox.kaist.ac.kr/flagship/dialogid', 'http://ko.dbpedia.org/resource/' + str(dialogDB_result) ])
				userDB_json = {'user_id': result_json['user_id'], 'command': result_json['mode'], 'triple': new_triples}
				userDB_result = UserDBaccess(userDB_json)
				result_json['result'] = "triples registered"
			result_json['result'] = "dialog registered"
		else:
			print("db_type error")
			result_json['result'] = "db_type error"
			return jsonify(result_json)
	else:
		print("mode error")
		result_json['result'] = "mode error"
		return jsonify(result_json)

	return jsonify(result_json)

if __name__ == "__main__":
	app.run()
