from typing import Tuple, List, Dict
import os
import json
import jpype
from flask import Flask, request
import datetime
import requests
from urllib.parse import urlencode
import urllib3

HOME_DIRECTORY = "/root/flagship/"
DOCKER_EXEC_PREFIX = "docker exec stardog_"

USER_FILE = "users.txt"
USERS = []

USER_ID_PREFIX = "http://kbox.kaist.ac.kr/flagship/userid/"
OBJECT_PREFIX = "http://ko.dbpedia.org/resource/"
PROPERTY_PREFIX = "http://ko.dbpedia.org/property/"
headers = {
	'Content-Type': 'application/x-www-form-urlencoded, application/sparql-query, text/turtle',
	'Accept': 'text/turtle, application/rdf+xml, application/n-triples, application/trig, application/n-quads, '
			  'text/n3, application/trix, application/ld+json, '  # application/sparql-results+xml, '
			  'application/sparql-results+json, application/x-binary-rdf-results-table, text/boolean, text/csv, '
			  'text/tsv, text/tab-separated-values '
}
TARGET_DB = "userDB"


def writefile(obj, fname):
	with open(fname, "w", encoding="utf8") as f:
		for item in obj:
			f.write(item + "\n")


app = Flask(__name__)
with open(USER_FILE, encoding="utf8") as f:
	for line in f.readlines():
		USERS.append(line.strip())


def new_user(user_name: str) -> int:
	if user_name in ["my", "iterative"]: return 1
	print("CREATE USER: %s" % user_name)
	code = os.system("%s mkdir %s" % (DOCKER_EXEC_PREFIX, HOME_DIRECTORY + user_name))
	# code |= os.system("%s /root/stardog/bin/stardog-admin db create -o versioning.enabled=true -n %sDB /root/kbox/schema.owl" % (DOCKER_EXEC_PREFIX, user_name))
	USERS.append(user_name)
	writefile(USERS, USER_FILE)
	return code


def get_user(user_id: str):  # user id를 기반으로 user name 반환, 없을 경우 user id로 user name 생성
	if user_id not in USERS:
		new_user(user_id)
		user_name = None
	else:
		try:
			user_name = query(user_id, "SELECT ?o where { graph <http://kbox.kaist.ac.kr/username/" + user_id + "> { ?s <http://ko.dbpedia.org/property/user_name> ?o}}")["query_result"][0]["o"]["value"].replace(OBJECT_PREFIX, "")
		except Exception:

			import traceback
			traceback.print_exc()
			user_name = None
	return {"user_id": user_id, "user_name": user_name}


def query(user_name: str, query: str) -> Dict:  # user name과 query를 받아 query 실행. query에 graph iri 명시 필요
	server = "http://kbox.kaist.ac.kr:5820/%s/" % TARGET_DB
	values = urlencode({"query": query})
	#http = urllib3.PoolManager()
	url = server + 'query?' + values
	r = requests.get(url, headers=headers)
	#r = http.request('GET', url, headers=headers)
	request = r.json()
	#request = json.loads(r.data.decode('UTF-8'))
	if 'ASK' in query:
		result_list = request['boolean']
	elif 'SELECT' in query:
		result_list = request['results']['bindings']
	else:
		result_list = None

	return {"user_id": user_name, "query_result": result_list}


def register_triple(user_name: str, *triple: Tuple[str, str, str]) -> None:  # user name graph iri에 트리플 등록
	def converter(s, p, o):
		#return "\t".join(
		#	["<" + OBJECT_PREFIX + s + ">", "<" + PROPERTY_PREFIX + p + ">", "<" + OBJECT_PREFIX + o + ">", "."])
		return "\t".join(
			["<" + s + ">", "<" + p + ">", "<" + o + ">", "."])

	#fname = str(datetime.datetime.now()).replace(" ", "").replace(":", "_") + ".ttl"
	fname = user_name + ".ttl"
	'''
	try:
		f1 = open(fname, "r", encoding="utf-8")
		tmp_lines = f1.readlines()
		print("aaaaa")
		print(tmp_lines)
		f1.close()
		f2 = open(fname, "w", encoding="utf-8")
		for tmp_line in tmp_lines:
			print(tmp_line)
			f2.write(tmp_line + "\n")
	except:
		print("bbbbb")
		f2 = open(fname, "w", encoding="utf-8")
	'''
	f = open(fname, "a+", encoding="utf-8")
	for line in map(lambda x: converter(*x), triple):
		f.write(line + "\n")
	f.close()
	code = os.system("docker cp %s stardog_:/root/flagship/%s/%s" % (fname, user_name, fname))
	code |= os.system(
		"""docker exec stardog_ /root/stardog/bin/stardog vcs commit --add /root/flagship/%s/%s -m 'user %s commited %s' -g "http://kbox.kaist.ac.kr/username/%s" %s""" % (
			user_name, fname, user_name, fname, user_name, TARGET_DB))
	#os.remove(fname)
	return True


jpype.startJVM(jpype.getDefaultJVMPath())


@app.route("/flagship", methods=["POST"])
def main():
	jpype.attachThreadToJVM()
	print(request)
	print(request.json)
	text = request.json
	return run(text)


def run(j):
	user_id = j["user_id"]
	command = j["command"]

	if command == "LOGIN":
		return get_user(user_id)
	elif command == "QUERY":
		return query(user_id, j["query"])
	elif command == "REGISTER":
		code = register_triple(user_id, *j["triple"])
	else:
		return "INVALID COMMAND: %s" % command

	return str(not code)
